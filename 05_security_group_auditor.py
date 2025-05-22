import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to audit security groups for risky configurations.
    
    This function scans all security groups in the specified regions and
    identifies potentially risky configurations, such as:
    - Open access to sensitive ports (22, 3389, etc.)
    - Overly permissive rules (0.0.0.0/0)
    - Unused security groups
    
    Environment Variables:
    - REGIONS: Comma-separated list of AWS regions to scan (default: current region)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - HIGH_RISK_PORTS: Comma-separated list of high-risk ports (default: 22,3389,1433,3306,5432)
    """
    # Get configuration from environment variables
    regions_str = os.environ.get('REGIONS', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    high_risk_ports_str = os.environ.get('HIGH_RISK_PORTS', '22,3389,1433,3306,5432')
    
    # If no regions specified, use the current region
    if not regions_str:
        regions = [boto3.session.Session().region_name]
    else:
        regions = [region.strip() for region in regions_str.split(',')]
    
    high_risk_ports = [int(port.strip()) for port in high_risk_ports_str.split(',')]
    
    findings = {}
    
    for region in regions:
        ec2 = boto3.client('ec2', region_name=region)
        findings[region] = {
            'open_to_world': [],
            'high_risk_ports': [],
            'unused_groups': []
        }
        
        # Get all security groups
        security_groups = ec2.describe_security_groups()['SecurityGroups']
        
        # Get network interfaces to check for unused security groups
        network_interfaces = ec2.describe_network_interfaces()
        used_sg_ids = set()
        
        for interface in network_interfaces['NetworkInterfaces']:
            for sg in interface['Groups']:
                used_sg_ids.add(sg['GroupId'])
        
        # Analyze each security group
        for sg in security_groups:
            sg_id = sg['GroupId']
            sg_name = sg['GroupName']
            
            # Check if security group is unused
            if sg_id not in used_sg_ids and sg_name != 'default':
                findings[region]['unused_groups'].append({
                    'id': sg_id,
                    'name': sg_name,
                    'vpc_id': sg.get('VpcId', 'N/A')
                })
            
            # Check inbound rules
            for rule in sg.get('IpPermissions', []):
                from_port = rule.get('FromPort', 0)
                to_port = rule.get('ToPort', 0)
                ip_ranges = rule.get('IpRanges', [])
                
                for ip_range in ip_ranges:
                    cidr = ip_range.get('CidrIp', '')
                    
                    # Check for rules open to the world
                    if cidr == '0.0.0.0/0':
                        findings[region]['open_to_world'].append({
                            'id': sg_id,
                            'name': sg_name,
                            'from_port': from_port,
                            'to_port': to_port,
                            'protocol': rule.get('IpProtocol', 'all')
                        })
                        
                        # Check for high-risk ports
                        if any(from_port <= port <= to_port for port in high_risk_ports):
                            findings[region]['high_risk_ports'].append({
                                'id': sg_id,
                                'name': sg_name,
                                'from_port': from_port,
                                'to_port': to_port,
                                'protocol': rule.get('IpProtocol', 'all'),
                                'cidr': cidr
                            })
    
    # Send findings to SNS if configured
    if sns_topic_arn:
        sns = boto3.client('sns')
        
        # Count total issues
        total_issues = sum(
            len(findings[region]['open_to_world']) + 
            len(findings[region]['high_risk_ports']) + 
            len(findings[region]['unused_groups'])
            for region in findings
        )
        
        message = {
            'subject': f"Security Group Audit - {total_issues} issues found",
            'timestamp': datetime.now().isoformat(),
            'findings': findings
        }
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message, indent=2),
            Subject=message['subject']
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Security group audit completed',
            'findings': findings
        })
    }