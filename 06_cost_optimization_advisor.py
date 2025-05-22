import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to identify cost optimization opportunities.
    
    This function analyzes AWS resources to find cost optimization opportunities:
    - Underutilized EC2 instances
    - Unattached EBS volumes
    - Idle load balancers
    - Unused Elastic IPs
    
    Environment Variables:
    - REGION: AWS region to analyze (default: current region)
    - CPU_THRESHOLD: CPU utilization threshold for underutilized instances (default: 10)
    - DAYS_TO_ANALYZE: Number of days of metrics to analyze (default: 14)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', boto3.session.Session().region_name)
    cpu_threshold = float(os.environ.get('CPU_THRESHOLD', 10))
    days_to_analyze = int(os.environ.get('DAYS_TO_ANALYZE', 14))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    ec2 = boto3.client('ec2', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    elb = boto3.client('elbv2', region_name=region)
    
    recommendations = {
        'underutilized_instances': [],
        'unattached_volumes': [],
        'idle_load_balancers': [],
        'unused_elastic_ips': []
    }
    
    # Find underutilized EC2 instances
    instances = ec2.describe_instances(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
    )
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_to_analyze)
    
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            instance_type = instance['InstanceType']
            
            # Get average CPU utilization
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,  # Daily average
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                avg_cpu = sum(point['Average'] for point in response['Datapoints']) / len(response['Datapoints'])
                
                if avg_cpu < cpu_threshold:
                    recommendations['underutilized_instances'].append({
                        'instance_id': instance_id,
                        'instance_type': instance_type,
                        'avg_cpu_utilization': avg_cpu,
                        'days_analyzed': days_to_analyze
                    })
    
    # Find unattached EBS volumes
    volumes = ec2.describe_volumes()
    
    for volume in volumes['Volumes']:
        if len(volume['Attachments']) == 0:
            recommendations['unattached_volumes'].append({
                'volume_id': volume['VolumeId'],
                'volume_type': volume['VolumeType'],
                'size_gb': volume['Size'],
                'created': volume['CreateTime'].isoformat()
            })
    
    # Find idle load balancers
    load_balancers = elb.describe_load_balancers()
    
    for lb in load_balancers['LoadBalancers']:
        lb_arn = lb['LoadBalancerArn']
        lb_name = lb['LoadBalancerName']
        
        # Get request count
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/ApplicationELB',
            MetricName='RequestCount',
            Dimensions=[{'Name': 'LoadBalancer', 'Value': lb_arn.split('/')[-1]}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,  # Daily
            Statistics=['Sum']
        )
        
        if not response['Datapoints'] or all(point['Sum'] < 10 for point in response['Datapoints']):
            recommendations['idle_load_balancers'].append({
                'load_balancer_name': lb_name,
                'load_balancer_arn': lb_arn,
                'days_analyzed': days_to_analyze
            })
    
    # Find unused Elastic IPs
    addresses = ec2.describe_addresses()
    
    for address in addresses['Addresses']:
        if 'AssociationId' not in address:
            recommendations['unused_elastic_ips'].append({
                'allocation_id': address.get('AllocationId', 'N/A'),
                'public_ip': address['PublicIp']
            })
    
    # Calculate potential savings
    savings = {
        'estimated_monthly_savings': 0
    }
    
    # Send to SNS if configured
    if sns_topic_arn:
        sns = boto3.client('sns')
        
        message = {
            'subject': "AWS Cost Optimization Recommendations",
            'timestamp': datetime.now().isoformat(),
            'recommendations': recommendations,
            'potential_savings': savings
        }
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message, indent=2),
            Subject=message['subject']
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Cost optimization analysis completed',
            'recommendations': recommendations,
            'potential_savings': savings
        })
    }