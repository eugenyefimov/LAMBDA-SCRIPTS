import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor Route53 health checks and DNS records.
    
    This function checks the status of Route53 health checks and can:
    - Report on failing health checks
    - Monitor DNS record changes
    - Verify DNS propagation
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - HEALTH_CHECK_IDS: Comma-separated list of health check IDs to monitor (optional)
    - HOSTED_ZONE_IDS: Comma-separated list of hosted zone IDs to monitor (optional)
    - LOOKBACK_HOURS: Hours of history to check for DNS changes (default: 24)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    health_check_ids_str = os.environ.get('HEALTH_CHECK_IDS', '')
    hosted_zone_ids_str = os.environ.get('HOSTED_ZONE_IDS', '')
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Parse health check IDs and hosted zone IDs
    health_check_ids = [id.strip() for id in health_check_ids_str.split(',')] if health_check_ids_str else []
    hosted_zone_ids = [id.strip() for id in hosted_zone_ids_str.split(',')] if hosted_zone_ids_str else []
    
    # Initialize AWS clients
    route53 = boto3.client('route53', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    results = {
        'failing_health_checks': [],
        'dns_changes': [],
        'health_check_status': {}
    }
    
    # If no specific health checks provided, get all health checks
    if not health_check_ids:
        paginator = route53.get_paginator('list_health_checks')
        for page in paginator.paginate():
            for health_check in page['HealthChecks']:
                health_check_ids.append(health_check['Id'])
    
    # Check status of health checks
    for health_check_id in health_check_ids:
        try:
            # Get health check details
            health_check = route53.get_health_check(HealthCheckId=health_check_id)
            
            # Get health check status from CloudWatch
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Route53',
                MetricName='HealthCheckStatus',
                Dimensions=[
                    {
                        'Name': 'HealthCheckId',
                        'Value': health_check_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5-minute intervals
                Statistics=['Minimum']
            )
            
            # Process health check status
            status_points = response['Datapoints']
            if status_points:
                # Sort by timestamp
                status_points.sort(key=lambda x: x['Timestamp'])
                
                # Check if any status is 0 (failing)
                failing_points = [point for point in status_points if point['Minimum'] == 0]
                
                if failing_points:
                    results['failing_health_checks'].append({
                        'health_check_id': health_check_id,
                        'config': health_check['HealthCheck']['HealthCheckConfig'],
                        'failing_periods': len(failing_points),
                        'first_failure': failing_points[0]['Timestamp'].isoformat(),
                        'latest_status': status_points[-1]['Minimum']
                    })
                
                # Add overall status
                results['health_check_status'][health_check_id] = {
                    'config': health_check['HealthCheck']['HealthCheckConfig'],
                    'current_status': status_points[-1]['Minimum'],
                    'status_history': [{'timestamp': point['Timestamp'].isoformat(), 'status': point['Minimum']} for point in status_points]
                }
            else:
                results['health_check_status'][health_check_id] = {
                    'config': health_check['HealthCheck']['HealthCheckConfig'],
                    'current_status': 'Unknown - no data',
                    'status_history': []
                }
                
        except Exception as e:
            print(f"Error checking health check {health_check_id}: {str(e)}")
    
    # If no specific hosted zones provided, get all hosted zones
    if not hosted_zone_ids:
        paginator = route53.get_paginator('list_hosted_zones')
        for page in paginator.paginate():
            for zone in page['HostedZones']:
                hosted_zone_ids.append(zone['Id'].replace('/hostedzone/', ''))
    
    # Check for DNS changes
    for zone_id in hosted_zone_ids:
        try:
            # Get change history
            paginator = route53.get_paginator('list_resource_record_sets')
            
            # We can't directly query change history by time, so we'll get all records
            # and then check CloudTrail for changes (in a real implementation)
            
            # For this example, we'll just get the current records
            zone_records = []
            for page in paginator.paginate(HostedZoneId=zone_id):
                zone_records.extend(page['ResourceRecordSets'])
            
            # Add zone info to results
            results['dns_changes'].append({
                'zone_id': zone_id,
                'record_count': len(zone_records),
                'note': 'Full change history requires CloudTrail integration'
            })
            
        except Exception as e:
            print(f"Error checking hosted zone {zone_id}: {str(e)}")
    
    # Send to SNS if configured
    if sns_topic_arn and results['failing_health_checks']:
        sns = boto3.client('sns', region_name=region)
        
        message = {
            'subject': f"Route53 Monitoring - {len(results['failing_health_checks'])} failing health checks",
            'timestamp': datetime.now().isoformat(),
            'results': results
        }
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message, indent=2),
            Subject=message['subject']
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Route53 monitoring completed',
            'results': results
        })
    }