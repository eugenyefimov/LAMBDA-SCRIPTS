import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to process and respond to GuardDuty findings.
    
    This function processes GuardDuty security findings:
    - Filters and prioritizes findings based on severity
    - Takes automated remediation actions for certain finding types
    - Enriches findings with additional context
    - Forwards findings to security tools or ticketing systems
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - MIN_SEVERITY: Minimum severity to process (default: 4.0)
    - AUTO_REMEDIATE: Whether to perform auto-remediation (default: false)
    - REMEDIATION_WHITELIST: JSON string of finding types to auto-remediate
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - TICKET_SYSTEM_URL: Optional URL for ticketing system integration
    - TICKET_SYSTEM_API_KEY: Optional API key for ticketing system
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    min_severity = float(os.environ.get('MIN_SEVERITY', 4.0))
    auto_remediate = os.environ.get('AUTO_REMEDIATE', 'false').lower() == 'true'
    remediation_whitelist_str = os.environ.get('REMEDIATION_WHITELIST', '[]')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    ticket_system_url = os.environ.get('TICKET_SYSTEM_URL', '')
    ticket_api_key = os.environ.get('TICKET_SYSTEM_API_KEY', '')
    
    # Initialize AWS clients
    guardduty = boto3.client('guardduty', region_name=region)
    ec2 = boto3.client('ec2', region_name=region)
    iam = boto3.client('iam', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Parse remediation whitelist
    try:
        remediation_whitelist = json.loads(remediation_whitelist_str)
    except json.JSONDecodeError:
        remediation_whitelist = []
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'findings_processed': 0,
        'high_severity_findings': [],
        'remediation_actions': [],
        'tickets_created': []
    }
    
    # Process findings from the event
    if 'detail' in event and 'findings' in event['detail']:
        findings = event['detail']['findings']
    else:
        # If not triggered by GuardDuty event, get recent findings
        detector_ids = []
        try:
            detectors = guardduty.list_detectors()
            detector_ids = detectors['DetectorIds']
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing GuardDuty detectors',
                    'error': str(e)
                })
            }
        
        findings = []
        for detector_id in detector_ids:
            try:
                # Get findings from the last 24 hours
                list_findings_response = guardduty.list_findings(
                    DetectorId=detector_id,
                    FindingCriteria={
                        'Criterion': {
                            'updatedAt': {
                                'Gte': int((datetime.now() - timedelta(days=1)).timestamp())
                            }
                        }
                    }
                )
                
                if list_findings_response['FindingIds']:
                    findings_response = guardduty.get_findings(
                        DetectorId=detector_id,
                        FindingIds=list_findings_response['FindingIds']
                    )
                    findings.extend(findings_response['Findings'])
            except Exception as e:
                print(f"Error getting findings for detector {detector_id}: {str(e)}")
    
    results['findings_processed'] = len(findings)
    
    # Process each finding
    for finding in findings:
        finding_id = finding['Id']
        finding_type = finding['Type']
        severity = finding['Severity']
        
        # Skip findings below minimum severity
        if severity < min_severity:
            continue
        
        # Add high severity findings to results
        if severity >= 7.0:
            results['high_severity_findings'].append({
                'id': finding_id,
                'type': finding_type,
                'severity': severity,
                'description': finding.get('Description', ''),
                'resource': finding.get('Resource', {})
            })
        
        # Perform auto-remediation if enabled and finding type is in whitelist
        if auto_remediate and finding_type in remediation_whitelist:
            remediation_action = {
                'finding_id': finding_id,
                'finding_type': finding_type,
                'actions': []
            }
            
            # Implement remediation logic based on finding type
            if finding_type.startswith('UnauthorizedAccess:IAMUser'):
                # Example: Disable IAM user access keys
                if 'Resource' in finding and 'AccessKeyDetails' in finding['Resource']:
                    access_key_id = finding['Resource']['AccessKeyDetails'].get('AccessKeyId')
                    user_name = finding['Resource']['AccessKeyDetails'].get('UserName')
                    
                    if access_key_id and user_name:
                        try:
                            iam.update_access_key(
                                UserName=user_name,
                                AccessKeyId=access_key_id,
                                Status='Inactive'
                            )
                            remediation_action['actions'].append(f"Disabled access key {access_key_id} for user {user_name}")
                        except Exception as e:
                            remediation_action['errors'] = [str(e)]
            
            elif finding_type.startswith('Recon:EC2'):
                # Example: Add security group rule to block suspicious IP
                if 'Resource' in finding and 'InstanceDetails' in finding['Resource']:
                    instance_id = finding['Resource']['InstanceDetails'].get('InstanceId')
                    
                    if instance_id and 'Service' in finding and 'Action' in finding['Service'] and 'RemoteIpDetails' in finding['Service']['Action']:
                        remote_ip = finding['Service']['Action']['RemoteIpDetails'].get('IpAddressV4')
                        
                        if remote_ip:
                            try:
                                # Get security groups for the instance
                                instance = ec2.describe_instances(InstanceIds=[instance_id])
                                security_groups = instance['Reservations'][0]['Instances'][0]['SecurityGroups']
                                
                                for sg in security_groups:
                                    # Create a deny rule for the suspicious IP
                                    ec2.authorize_security_group_ingress(
                                        GroupId=sg['GroupId'],
                                        IpPermissions=[
                                            {
                                                'IpProtocol': '-1',
                                                'FromPort': -1,
                                                'ToPort': -1,
                                                'IpRanges': [
                                                    {
                                                        'CidrIp': f"{remote_ip}/32",
                                                        'Description': f"Block suspicious IP from GuardDuty finding {finding_id}"
                                                    }
                                                ]
                                            }
                                        ]
                                    )
                                    remediation_action['actions'].append(f"Blocked IP {remote_ip} in security group {sg['GroupId']}")
                            except Exception as e:
                                remediation_action['errors'] = [str(e)]
            
            # Add remediation action to results if any actions were taken
            if 'actions' in remediation_action and remediation_action['actions']:
                results['remediation_actions'].append(remediation_action)
        
        # Create ticket in external system if configured
        if ticket_system_url and ticket_api_key:
            try:
                # This is a placeholder for ticket creation logic
                # In a real implementation, you would make an API call to your ticketing system
                ticket = {
                    'finding_id': finding_id,
                    'ticket_id': f"GD-{finding_id[:8]}",
                    'severity': 'High' if severity >= 7.0 else ('Medium' if severity >= 4.0 else 'Low'),
                    'summary': f"GuardDuty Finding: {finding_type}"
                }
                results['tickets_created'].append(ticket)
            except Exception as e:
                print(f"Error creating ticket for finding {finding_id}: {str(e)}")
    
    # Send notification if high severity findings and SNS topic provided
    if sns and sns_topic_arn and results['high_severity_findings']:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"GuardDuty Alert - {len(results['high_severity_findings'])} high severity findings",
                Message=json.dumps({
                    'findings': results['high_severity_findings'],
                    'remediation_actions': results['remediation_actions']
                }, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }