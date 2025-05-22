import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Transfer Family services.
    
    This function monitors AWS Transfer Family (SFTP, FTPS, FTP) services:
    - Tracks file transfer activities
    - Monitors user access and authentication
    - Identifies failed transfers and connection attempts
    - Generates usage reports and alerts on abnormal patterns
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - SERVER_IDS: Optional comma-separated list of Transfer Family server IDs
    - LOOKBACK_HOURS: Hours of history to analyze (default: 24)
    - ALERT_THRESHOLD: Number of failed attempts to trigger alert (default: 5)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - S3_REPORT_BUCKET: Optional S3 bucket for storing reports
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    server_ids_str = os.environ.get('SERVER_IDS', '')
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    alert_threshold = int(os.environ.get('ALERT_THRESHOLD', 5))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    s3_report_bucket = os.environ.get('S3_REPORT_BUCKET', '')
    
    # Initialize AWS clients
    transfer = boto3.client('transfer', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    logs = boto3.client('logs', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    s3 = boto3.client('s3', region_name=region) if s3_report_bucket else None
    
    # Calculate time range for analysis
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'servers_analyzed': [],
        'transfer_statistics': {},
        'failed_transfers': [],
        'suspicious_activities': [],
        'recommendations': []
    }
    
    # Get list of Transfer Family servers
    server_ids = []
    if server_ids_str:
        server_ids = [server_id.strip() for server_id in server_ids_str.split(',')]
    else:
        try:
            paginator = transfer.get_paginator('list_servers')
            for page in paginator.paginate():
                for server in page['Servers']:
                    server_ids.append(server['ServerId'])
        except Exception as e:
            print(f"Error listing Transfer Family servers: {str(e)}")
    
    # Analyze each server
    for server_id in server_ids:
        try:
            # Get server details
            server_details = transfer.describe_server(ServerId=server_id)
            server_info = {
                'server_id': server_id,
                'endpoint': server_details['Server'].get('Endpoint', ''),
                'protocols': server_details['Server'].get('Protocols', []),
                'state': server_details['Server'].get('State', ''),
                'users_count': 0,
                'transfer_count': 0,
                'failed_transfers': 0
            }
            
            # Get users for this server
            users = []
            try:
                paginator = transfer.get_paginator('list_users')
                for page in paginator.paginate(ServerId=server_id):
                    users.extend(page['Users'])
                server_info['users_count'] = len(users)
            except Exception as e:
                print(f"Error listing users for server {server_id}: {str(e)}")
            
            # Get CloudWatch metrics for this server
            try:
                # Get file transfer metrics
                transfer_metrics = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Transfer',
                    MetricName='FilesIn',
                    Dimensions=[
                        {'Name': 'ServerId', 'Value': server_id}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                
                total_transfers = sum(point['Sum'] for point in transfer_metrics['Datapoints'])
                server_info['transfer_count'] = int(total_transfers)
                
                # Get failed file transfer metrics
                failed_metrics = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Transfer',
                    MetricName='FilesInFailed',
                    Dimensions=[
                        {'Name': 'ServerId', 'Value': server_id}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Sum']
                )
                
                total_failed = sum(point['Sum'] for point in failed_metrics['Datapoints'])
                server_info['failed_transfers'] = int(total_failed)
                
                # Check if failed transfers exceed threshold
                if total_failed >= alert_threshold:
                    results['failed_transfers'].append({
                        'server_id': server_id,
                        'failed_count': int(total_failed),
                        'time_period': f"{start_time.isoformat()} to {end_time.isoformat()}"
                    })
            
            except Exception as e:
                print(f"Error getting metrics for server {server_id}: {str(e)}")
            
            # Add server info to results
            results['servers_analyzed'].append(server_info)
            
            # Add to transfer statistics
            results['transfer_statistics'][server_id] = {
                'total_transfers': server_info['transfer_count'],
                'failed_transfers': server_info['failed_transfers'],
                'success_rate': (
                    (server_info['transfer_count'] - server_info['failed_transfers']) / 
                    server_info['transfer_count'] * 100 if server_info['transfer_count'] > 0 else 100
                )
            }
            
            # Generate recommendations
            if server_info['failed_transfers'] > 0:
                results['recommendations'].append({
                    'server_id': server_id,
                    'recommendation': 'Review server logs to identify causes of failed transfers',
                    'severity': 'Medium' if server_info['failed_transfers'] >= alert_threshold else 'Low'
                })
            
        except Exception as e:
            print(f"Error analyzing server {server_id}: {str(e)}")
    
    # Generate report and store in S3 if configured
    if s3 and s3_report_bucket:
        try:
            report_key = f"transfer-family-reports/report-{datetime.now().strftime('%Y-%m-%d-%H-%M')}.json"
            s3.put_object(
                Bucket=s3_report_bucket,
                Key=report_key,
                Body=json.dumps(results, indent=2, default=str),
                ContentType='application/json'
            )
            results['report_location'] = f"s3://{s3_report_bucket}/{report_key}"
        except Exception as e:
            print(f"Error storing report in S3: {str(e)}")
    
    # Send SNS notification if configured
    if sns and sns_topic_arn and results['failed_transfers']:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"AWS Transfer Family Monitor - Failed Transfers Detected",
                Message=json.dumps({
                    'timestamp': results['timestamp'],
                    'failed_transfers': results['failed_transfers'],
                    'recommendations': results['recommendations']
                }, indent=2, default=str)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }