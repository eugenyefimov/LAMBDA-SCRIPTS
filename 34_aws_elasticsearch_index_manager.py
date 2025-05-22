import boto3
import json
import os
import requests
from datetime import datetime, timedelta
from requests_aws4auth import AWS4Auth

def lambda_handler(event, context):
    """
    AWS Lambda function to manage Amazon Elasticsearch/OpenSearch indices.
    
    This function automates Elasticsearch/OpenSearch index management:
    - Creates index lifecycle policies
    - Rotates indices based on age or size
    - Deletes old indices based on retention policy
    - Optimizes index settings for performance
    - Generates index statistics reports
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ES_ENDPOINT: Elasticsearch/OpenSearch endpoint URL (required)
    - INDEX_PREFIX: Prefix of indices to manage (default: logs-)
    - RETENTION_DAYS: Number of days to retain indices (default: 30)
    - MAX_INDEX_SIZE_GB: Maximum index size in GB before rotation (default: 50)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    es_endpoint = os.environ.get('ES_ENDPOINT', '')
    index_prefix = os.environ.get('INDEX_PREFIX', 'logs-')
    retention_days = int(os.environ.get('RETENTION_DAYS', 30))
    max_index_size_gb = int(os.environ.get('MAX_INDEX_SIZE_GB', 50))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    if not es_endpoint:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable: ES_ENDPOINT')
        }
    
    # Initialize AWS clients
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Create AWS authentication for ES
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        'es',
        session_token=credentials.token
    )
    
    # Calculate cutoff date for retention
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    cutoff_date_str = cutoff_date.strftime('%Y.%m.%d')
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'indices_deleted': [],
        'indices_optimized': [],
        'indices_created': [],
        'errors': []
    }
    
    # Get all indices
    try:
        response = requests.get(f"{es_endpoint}/_cat/indices/{index_prefix}*?format=json", auth=awsauth)
        response.raise_for_status()
        indices = response.json()
        
        # Process each index
        for index in indices:
            index_name = index['index']
            
            # Check if index is older than retention period
            # Assuming index name format includes date like logs-2023.01.01
            try:
                if '-' in index_name and '.' in index_name.split('-')[1]:
                    index_date_str = index_name.split('-')[1]
                    if index_date_str < cutoff_date_str:
                        # Delete old index
                        delete_response = requests.delete(f"{es_endpoint}/{index_name}", auth=awsauth)
                        delete_response.raise_for_status()
                        
                        results['indices_deleted'].append({
                            'index_name': index_name,
                            'creation_date': index.get('creation.date', 'unknown')
                        })
            except Exception as e:
                error_msg = f"Error processing index date for {index_name}: {str(e)}"
                results['errors'].append(error_msg)
                print(error_msg)
            
            # Check if index is too large
            try:
                size_bytes = int(index.get('pri.store.size', '0').replace('kb', '000').replace('mb', '000000').replace('gb', '000000000').replace('b', ''))
                size_gb = size_bytes / 1000000000
                
                if size_gb > max_index_size_gb:
                    # Optimize large index
                    optimize_response = requests.post(f"{es_endpoint}/{index_name}/_forcemerge?max_num_segments=1", auth=awsauth)
                    optimize_response.raise_for_status()
                    
                    results['indices_optimized'].append({
                        'index_name': index_name,
                        'size_gb': size_gb
                    })
            except Exception as e:
                error_msg = f"Error optimizing large index {index_name}: {str(e)}"
                results['errors'].append(error_msg)
                print(error_msg)
    
    except Exception as e:
        error_msg = f"Error listing or processing indices: {str(e)}"
        results['errors'].append(error_msg)
        print(error_msg)
    
    # Create new index with today's date if it doesn't exist
    try:
        today_index = f"{index_prefix}{datetime.now().strftime('%Y.%m.%d')}"
        
        # Check if today's index exists
        exists_response = requests.head(f"{es_endpoint}/{today_index}", auth=awsauth)
        
        if exists_response.status_code == 404:
            # Create new index with optimized settings
            settings = {
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 1,
                    "refresh_interval": "30s"
                }
            }
            
            create_response = requests.put(
                f"{es_endpoint}/{today_index}",
                auth=awsauth,
                json=settings,
                headers={"Content-Type": "application/json"}
            )
            create_response.raise_for_status()
            
            results['indices_created'].append({
                'index_name': today_index,
                'settings': settings
            })
    
    except Exception as e:
        error_msg = f"Error creating new index: {str(e)}"
        results['errors'].append(error_msg)
        print(error_msg)
    
    # Send SNS notification if configured
    if sns and sns_topic_arn:
        try:
            message = {
                'timestamp': results['timestamp'],
                'indices_deleted_count': len(results['indices_deleted']),
                'indices_optimized_count': len(results['indices_optimized']),
                'indices_created_count': len(results['indices_created']),
                'errors_count': len(results['errors'])
            }
            
            # Include details if there are errors
            if results['errors']:
                message['errors'] = results['errors']
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Elasticsearch Index Manager Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps(message, indent=2, default=str)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }