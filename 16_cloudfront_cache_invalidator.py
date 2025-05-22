import boto3
import json
import os
import time
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to automatically invalidate CloudFront cache when content changes.
    
    This function can be triggered by S3 events or manually to invalidate CloudFront
    distribution caches when content is updated. It supports:
    - Path-based invalidation
    - Wildcard invalidation patterns
    - Batch invalidation to reduce costs
    
    Environment Variables:
    - DISTRIBUTION_ID: CloudFront distribution ID (required)
    - PATHS_TO_INVALIDATE: Comma-separated paths to invalidate (default: /*)
    - REGION: AWS region to operate in (default: us-east-1)
    - CALLER_REFERENCE_PREFIX: Prefix for caller reference (default: lambda-invalidation)
    """
    # Get configuration from environment variables
    distribution_id = os.environ.get('DISTRIBUTION_ID')
    paths_to_invalidate_str = os.environ.get('PATHS_TO_INVALIDATE', '/*')
    region = os.environ.get('REGION', 'us-east-1')
    caller_reference_prefix = os.environ.get('CALLER_REFERENCE_PREFIX', 'lambda-invalidation')
    
    # Validate required parameters
    if not distribution_id:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Missing required environment variable: DISTRIBUTION_ID'
            })
        }
    
    # Parse paths to invalidate
    paths_to_invalidate = [path.strip() for path in paths_to_invalidate_str.split(',')]
    
    # Check if event contains S3 information
    if event.get('Records') and event['Records'][0].get('eventSource') == 'aws:s3':
        # Extract paths from S3 event
        s3_paths = []
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            # Format path for CloudFront (add leading slash)
            s3_paths.append('/' + key)
        
        # Use S3 paths if available, otherwise use configured paths
        if s3_paths:
            paths_to_invalidate = s3_paths
    
    # Initialize CloudFront client
    cloudfront = boto3.client('cloudfront', region_name=region)
    
    # Create a unique caller reference
    timestamp = int(time.time())
    caller_reference = f"{caller_reference_prefix}-{timestamp}"
    
    try:
        # Create invalidation
        response = cloudfront.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': len(paths_to_invalidate),
                    'Items': paths_to_invalidate
                },
                'CallerReference': caller_reference
            }
        )
        
        invalidation_id = response['Invalidation']['Id']
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'CloudFront invalidation created successfully',
                'distribution_id': distribution_id,
                'invalidation_id': invalidation_id,
                'paths_invalidated': paths_to_invalidate,
                'timestamp': datetime.now().isoformat()
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error creating CloudFront invalidation',
                'error': str(e),
                'distribution_id': distribution_id,
                'paths_attempted': paths_to_invalidate
            })
        }