import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS AppSync API health and performance.
    
    This function can:
    - Check the status of AppSync APIs.
    - Monitor metrics like latency, error rates.
    - Send notifications for issues.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - APPSYNC_API_IDS: Comma-separated list of AppSync API IDs to monitor.
    - SNS_TOPIC_ARN: SNS topic ARN for notifications of API issues.
    - CW_METRIC_NAMESPACE: CloudWatch metric namespace for AppSync (default: AWS/AppSync)
    - LATENCY_THRESHOLD_MS: Optional latency threshold in milliseconds for p90/p95/p99.
    - ERROR_RATE_THRESHOLD_PERCENT: Optional 4XX/5XX error rate threshold.
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    appsync_api_ids_str = os.environ.get('APPSYNC_API_IDS')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    # CloudWatch metric related (optional, for deeper monitoring)
    # cw_metric_namespace = os.environ.get('CW_METRIC_NAMESPACE', 'AWS/AppSync')
    # latency_threshold_ms = os.environ.get('LATENCY_THRESHOLD_MS')
    # error_rate_threshold_percent = os.environ.get('ERROR_RATE_THRESHOLD_PERCENT')


    if not appsync_api_ids_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required environment variable: APPSYNC_API_IDS')}
    if not sns_topic_arn: # Assuming notifications are key for a monitor
        return {'statusCode': 400, 'body': json.dumps('Missing required environment variable: SNS_TOPIC_ARN')}

    appsync_api_ids = [api_id.strip() for api_id in appsync_api_ids_str.split(',')]

    # Initialize AWS clients
    appsync_client = boto3.client('appsync', region_name=region)
    # cloudwatch_client = boto3.client('cloudwatch', region_name=region) # For metric checks
    sns = boto3.client('sns', region_name=region)
    
    monitor_results = {
        'timestamp': datetime.now().isoformat(),
        'problem_apis': [],
        'checked_apis_count': len(appsync_api_ids),
        'errors': []
    }

    for api_id in appsync_api_ids:
        try:
            # Basic check: Get GraphQL API details
            # A more thorough check would involve querying CloudWatch metrics for errors, latency etc.
            # This example primarily checks if the API exists and basic info.
            api_details = appsync_client.get_graphql_api(apiId=api_id)
            api_name = api_details['graphqlApi']['name']
            
            # Placeholder for more advanced checks (e.g., CloudWatch metrics)
            # For example, check 5XXError metric from CloudWatch
            # This would require more complex logic to define "problematic"
            # For simplicity, this script will just confirm the API is accessible.
            # If an API is deleted or inaccessible, get_graphql_api would raise an exception.

            # If you wanted to check schema status or other specific details:
            # schema_status = appsync_client.get_schema_creation_status(apiId=api_id)['status']
            # if schema_status != 'SUCCESS' and schema_status != 'ACTIVE':
            #     problem_info = {
            #         'ApiId': api_id,
            #         'ApiName': api_name,
            #         'Issue': f"Schema status is {schema_status}"
            #     }
            #     monitor_results['problem_apis'].append(problem_info)
            #     message = f"AWS AppSync API Alert for {api_name} (ID: {api_id}): Schema status is {schema_status}."
            #     sns.publish(TopicArn=sns_topic_arn, Message=message, Subject=f"AppSync API Issue: {api_name}")

            # For now, we assume if get_graphql_api succeeds, the API is "okay" at a basic level.
            # Real monitoring would involve CloudWatch metrics.

        except appsync_client.exceptions.NotFoundException:
            error_msg = f"AppSync API ID {api_id} not found."
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_apis'].append({'ApiId': api_id, 'Issue': 'Not Found'})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"AppSync API Issue: {api_id} Not Found")
        except Exception as e_api:
            error_msg = f"Error processing AppSync API {api_id}: {str(e_api)}"
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_apis'].append({'ApiId': api_id, 'Issue': str(e_api)})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"AppSync API Error: {api_id}")

    if not monitor_results['problem_apis'] and not monitor_results['errors']:
         # Optionally send a success/heartbeat message
        pass

    return {
        'statusCode': 200,
        'body': json.dumps(monitor_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['APPSYNC_API_IDS'] = 'your-api-id-1,your-api-id-2' # Replace with actual AppSync API IDs
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    # Note: AppSync API calls require specific permissions.
    print(lambda_handler({}, {}))