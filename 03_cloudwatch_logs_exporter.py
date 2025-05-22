import boto3
import json
import os
import gzip
from datetime import datetime, timedelta
from io import BytesIO

def lambda_handler(event, context):
    """
    AWS Lambda function to export CloudWatch logs to S3.
    
    This function exports logs from specified CloudWatch log groups to an S3 bucket.
    It can be scheduled to run periodically to archive logs.
    
    Environment Variables:
    - LOG_GROUPS: Comma-separated list of log group names to export
    - S3_BUCKET: Target S3 bucket for log exports
    - S3_PREFIX: Prefix for S3 objects (default: 'cloudwatch-logs/')
    - DAYS_AGO: Number of days in the past to start export (default: 1)
    - EXPORT_DURATION_HOURS: Duration of logs to export in hours (default: 24)
    """
    # Get configuration from environment variables
    log_groups_str = os.environ.get('LOG_GROUPS', '')
    s3_bucket = os.environ.get('S3_BUCKET', '')
    s3_prefix = os.environ.get('S3_PREFIX', 'cloudwatch-logs/')
    days_ago = int(os.environ.get('DAYS_AGO', 1))
    export_duration_hours = int(os.environ.get('EXPORT_DURATION_HOURS', 24))
    
    if not log_groups_str or not s3_bucket:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variables: LOG_GROUPS and S3_BUCKET')
        }
    
    log_groups = [group.strip() for group in log_groups_str.split(',')]
    
    # Calculate time range for export
    end_time = datetime.now() - timedelta(days=days_ago)
    start_time = end_time - timedelta(hours=export_duration_hours)
    
    # Convert to milliseconds since epoch
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)
    
    logs_client = boto3.client('logs')
    export_tasks = []
    
    for log_group in log_groups:
        # Create a unique task name
        task_name = f"export-{log_group.replace('/', '-')}-{end_time.strftime('%Y-%m-%d-%H-%M-%S')}"
        
        # Format the destination path in S3
        date_prefix = end_time.strftime('%Y/%m/%d')
        destination = f"{s3_prefix}{log_group}/{date_prefix}/"
        
        try:
            response = logs_client.create_export_task(
                taskName=task_name,
                logGroupName=log_group,
                fromTime=start_time_ms,
                to=end_time_ms,
                destination=s3_bucket,
                destinationPrefix=destination
            )
            
            export_tasks.append({
                'logGroup': log_group,
                'taskId': response['taskId'],
                'destination': f"s3://{s3_bucket}/{destination}"
            })
            
        except Exception as e:
            export_tasks.append({
                'logGroup': log_group,
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Export tasks created',
            'timeRange': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            },
            'exportTasks': export_tasks
        })
    }