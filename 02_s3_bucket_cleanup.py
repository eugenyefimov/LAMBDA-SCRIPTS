import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to clean up S3 buckets by deleting objects older than a specified age.
    
    This function scans specified S3 buckets and deletes objects that are older than
    the retention period defined in environment variables or bucket tags.
    
    Environment Variables:
    - DEFAULT_RETENTION_DAYS: Default number of days to retain objects (default: 30)
    - TARGET_BUCKETS: Comma-separated list of bucket names to clean up (optional)
    - RETENTION_TAG: Name of the tag that specifies retention period (default: 'RetentionDays')
    """
    # Get configuration from environment variables
    default_retention_days = int(os.environ.get('DEFAULT_RETENTION_DAYS', '30'))
    retention_tag = os.environ.get('RETENTION_TAG', 'RetentionDays')
    target_buckets = os.environ.get('TARGET_BUCKETS', '')
    
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    # Get list of buckets to process
    buckets_to_process = []
    if target_buckets:
        buckets_to_process = target_buckets.split(',')
    else:
        response = s3_client.list_buckets()
        buckets_to_process = [bucket['Name'] for bucket in response['Buckets']]
    
    deletion_results = {}
    
    for bucket_name in buckets_to_process:
        try:
            # Check if bucket has a custom retention policy tag
            retention_days = default_retention_days
            try:
                bucket_tagging = s3_client.get_bucket_tagging(Bucket=bucket_name)
                for tag in bucket_tagging['TagSet']:
                    if tag['Key'] == retention_tag and tag['Value'].isdigit():
                        retention_days = int(tag['Value'])
            except s3_client.exceptions.ClientError:
                # Bucket might not have tags
                pass
            
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Get and delete old objects
            bucket = s3_resource.Bucket(bucket_name)
            deleted_count = 0
            
            for obj in bucket.objects.all():
                if obj.last_modified < cutoff_date:
                    obj.delete()
                    deleted_count += 1
            
            deletion_results[bucket_name] = {
                'retention_days': retention_days,
                'deleted_objects': deleted_count
            }
            
        except Exception as e:
            deletion_results[bucket_name] = {
                'error': str(e)
            }
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'S3 bucket cleanup completed',
            'results': deletion_results
        })
    }