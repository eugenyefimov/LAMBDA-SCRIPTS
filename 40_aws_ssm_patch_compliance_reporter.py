import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to report on Systems Manager patch compliance.
    
    This function analyzes SSM patch compliance data:
    - Identifies non-compliant instances
    - Generates compliance reports by patch group
    - Tracks patch compliance trends over time
    - Optionally triggers patch installation for non-compliant instances
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - PATCH_GROUPS: Optional comma-separated list of patch groups to analyze
    - COMPLIANCE_THRESHOLD: Minimum compliance percentage threshold (default: 90)
    - AUTO_PATCH: Whether to trigger patching for non-compliant instances (default: false)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - S3_REPORT_BUCKET: Optional S3 bucket for storing reports
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    patch_groups_str = os.environ.get('PATCH_GROUPS', '')
    compliance_threshold = float(os.environ.get('COMPLIANCE_THRESHOLD', 90.0))
    auto_patch = os.environ.get('AUTO_PATCH', 'false').lower() == 'true'
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    s3_report_bucket = os.environ.get('S3_REPORT_BUCKET', '')
    
    # Initialize AWS clients
    ssm = boto3.client('ssm', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    s3 = boto3.client('s3', region_name=region) if s3_report_bucket else None
    
    # Parse patch groups
    patch_groups = [group.strip() for group in patch_groups_str.split(',')] if patch_groups_str else []
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'overall_compliance': {
            'compliant_instances': 0,
            'non_compliant_instances': 0,
            'compliance_percentage': 0.0
        },
        'patch_group_compliance': {},
        'non_compliant_instances': [],
        'patching_actions': []
    }
    
    # If no patch groups specified, get all patch groups
    if not patch_groups:
        try:
            response = ssm.describe_patch_groups()
            patch_groups = [group['PatchGroup'] for group in response['Mappings']]
        except Exception as e:
            print(f"Error getting patch groups: {str(e)}")
    
    total_instances = 0
    compliant_instances = 0
    
    # Analyze each patch group
    for patch_group in patch_groups:
        try:
            # Get patch group compliance summary
            response = ssm.describe_patch_group_state(PatchGroup=patch_group)
            
            group_total = response.get('Instances', 0)
            group_compliant = response.get('InstancesWithInstalledPatches', 0) - response.get('InstancesWithMissingPatches', 0)
            group_compliance_pct = (group_compliant / group_total * 100) if group_total > 0 else 0
            
            results['patch_group_compliance'][patch_group] = {
                'total_instances': group_total,
                'compliant_instances': group_compliant,
                'non_compliant_instances': group_total - group_compliant,
                'compliance_percentage': round(group_compliance_pct, 2)
            }
            
            total_instances += group_total
            compliant_instances += group_compliant
            
            # Get non-compliant instances in this patch group
            if group_total - group_compliant > 0:
                # Get instances in this patch group
                paginator = ssm.get_paginator('describe_instance_information')
                instances = []
                
                for page in paginator.paginate(Filters=[{'Key': 'PatchGroup', 'Values': [patch_group]}]):
                    instances.extend(page['InstanceInformationList'])
                
                for instance in instances:
                    instance_id = instance['InstanceId']
                    
                    # Get patch compliance for this instance
                    compliance = ssm.describe_instance_patches(
                        InstanceId=instance_id,
                        Filters=[{'Key': 'State', 'Values': ['Missing', 'Failed']}]
                    )
                    
                    if 'Patches' in compliance and compliance['Patches']:
                        missing_patches = len(compliance['Patches'])
                        
                        if missing_patches > 0:
                            results['non_compliant_instances'].append({
                                'instance_id': instance_id,
                                'patch_group': patch_group,
                                'missing_patches': missing_patches,
                                'critical_patches': sum(1 for p in compliance['Patches'] if p.get('Severity', '') in ['Critical', 'Important']),
                                'last_scan_time': instance.get('LastAssociationExecutionDate', '')
                            })
                            
                            # Trigger patching if auto-patch is enabled
                            if auto_patch:
                                try:
                                    # Create a command to install missing patches
                                    command = ssm.send_command(
                                        InstanceIds=[instance_id],
                                        DocumentName='AWS-RunPatchBaseline',
                                        Parameters={
                                            'Operation': ['Install']
                                        },
                                        Comment=f"Auto-patching triggered by Lambda function for non-compliant instance"
                                    )
                                    
                                    results['patching_actions'].append({
                                        'instance_id': instance_id,
                                        'command_id': command['Command']['CommandId'],
                                        'status': 'Initiated'
                                    })
                                except Exception as e:
                                    print(f"Error triggering patching for instance {instance_id}: {str(e)}")
        except Exception as e:
            print(f"Error analyzing patch group {patch_group}: {str(e)}")
    
    # Calculate overall compliance
    if total_instances > 0:
        compliance_percentage = (compliant_instances / total_instances) * 100
        results['overall_compliance'] = {
            'compliant_instances': compliant_instances,
            'non_compliant_instances': total_instances - compliant_instances,
            'total_instances': total_instances,
            'compliance_percentage': round(compliance_percentage, 2)
        }
    
    # Generate report
    report = {
        'summary': {
            'timestamp': results['timestamp'],
            'overall_compliance': results['overall_compliance'],
            'patch_groups_analyzed': len(patch_groups),
            'non_compliant_instances': len(results['non_compliant_instances']),
            'patching_actions': len(results['patching_actions'])
        },
        'details': results
    }
    
    # Save report to S3 if bucket specified
    if s3 and s3_report_bucket:
        try:
            report_key = f"ssm-patch-compliance/report-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"
            s3.put_object(
                Bucket=s3_report_bucket,
                Key=report_key,
                Body=json.dumps(report, indent=2),
                ContentType='application/json'
            )
            results['report_location'] = f"s3://{s3_report_bucket}/{report_key}"
        except Exception as e:
            print(f"Error saving report to S3: {str(e)}")
    
    # Send notification if compliance is below threshold and SNS topic provided
    if sns and sns_topic_arn and results['overall_compliance']['compliance_percentage'] < compliance_threshold:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Patch Compliance Alert - {results['overall_compliance']['compliance_percentage']}% (below threshold of {compliance_threshold}%)",
                Message=json.dumps(report, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }