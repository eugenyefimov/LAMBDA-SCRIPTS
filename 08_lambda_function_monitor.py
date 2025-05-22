import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor other Lambda functions for errors and performance issues.
    
    This function analyzes CloudWatch metrics and logs for Lambda functions to identify:
    - Functions with high error rates
    - Functions with timeout issues
    - Functions with high execution duration
    - Functions with high memory usage
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ERROR_THRESHOLD: Error rate threshold percentage (default: 5)
    - DURATION_THRESHOLD: Duration threshold percentage of max timeout (default: 80)
    - MEMORY_THRESHOLD: Memory usage threshold percentage (default: 80)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    error_threshold = float(os.environ.get('ERROR_THRESHOLD', 5))
    duration_threshold = float(os.environ.get('DURATION_THRESHOLD', 80))
    memory_threshold = float(os.environ.get('MEMORY_THRESHOLD', 80))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    lambda_client = boto3.client('lambda', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    logs_client = boto3.client('logs', region_name=region)
    
    # Get all Lambda functions
    functions = []
    paginator = lambda_client.get_paginator('list_functions')
    for page in paginator.paginate():
        functions.extend(page['Functions'])
    
    # Calculate time range for metrics (last 24 hours)
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    issues = {
        'high_error_rate': [],
        'timeout_issues': [],
        'high_duration': [],
        'high_memory_usage': []
    }
    
    for function in functions:
        function_name = function['FunctionName']
        function_timeout = function['Timeout']
        function_memory = function['MemorySize']
        
        # Check for errors
        error_metric = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Errors',
            Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour
            Statistics=['Sum']
        )
        
        invocation_metric = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour
            Statistics=['Sum']
        )
        
        # Calculate error rate
        total_errors = sum(point['Sum'] for point in error_metric['Datapoints'])
        total_invocations = sum(point['Sum'] for point in invocation_metric['Datapoints'])
        
        if total_invocations > 0:
            error_rate = (total_errors / total_invocations) * 100
            if error_rate > error_threshold:
                issues['high_error_rate'].append({
                    'function_name': function_name,
                    'error_rate': error_rate,
                    'total_errors': total_errors,
                    'total_invocations': total_invocations
                })
        
        # Check for timeouts by looking at CloudWatch Logs
        try:
            log_group_name = f"/aws/lambda/{function_name}"
            
            # Check if log group exists
            try:
                logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
                
                # Search for timeout messages in the logs
                query = "filter @message like /Task timed out/ | stats count() as timeout_count"
                start_query_response = logs_client.start_query(
                    logGroupName=log_group_name,
                    startTime=int(start_time.timestamp()),
                    endTime=int(end_time.timestamp()),
                    queryString=query
                )
                
                query_id = start_query_response['queryId']
                
                # Wait for query to complete
                response = None
                while response is None or response['status'] == 'Running':
                    response = logs_client.get_query_results(queryId=query_id)
                    if response['status'] == 'Running':
                        import time
                        time.sleep(1)
                
                # Process results
                if response['results'] and len(response['results']) > 0:
                    for result in response['results']:
                        for field in result:
                            if field['field'] == 'timeout_count' and int(field['value']) > 0:
                                issues['timeout_issues'].append({
                                    'function_name': function_name,
                                    'timeout_count': int(field['value'])
                                })
            except logs_client.exceptions.ResourceNotFoundException:
                # Log group doesn't exist, skip
                pass
                
        except Exception as e:
            # Log the error but continue processing
            print(f"Error checking timeouts for {function_name}: {str(e)}")
        
        # Check for high duration
        duration_metric = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Duration',
            Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour
            Statistics=['Maximum', 'Average']
        )
        
        if duration_metric['Datapoints']:
            max_duration = max(point['Maximum'] for point in duration_metric['Datapoints'])
            avg_duration = sum(point['Average'] for point in duration_metric['Datapoints']) / len(duration_metric['Datapoints'])
            
            # Convert to percentage of timeout
            max_duration_percent = (max_duration / (function_timeout * 1000)) * 100
            
            if max_duration_percent > duration_threshold:
                issues['high_duration'].append({
                    'function_name': function_name,
                    'max_duration_ms': max_duration,
                    'avg_duration_ms': avg_duration,
                    'timeout_ms': function_timeout * 1000,
                    'max_duration_percent': max_duration_percent
                })
        
        # Check for high memory usage
        memory_metric = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='MemoryUtilization',
            Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour
            Statistics=['Maximum', 'Average']
        )
        
        if memory_metric['Datapoints']:
            max_memory_percent = max(point['Maximum'] for point in memory_metric['Datapoints'])
            avg_memory_percent = sum(point['Average'] for point in memory_metric['Datapoints']) / len(memory_metric['Datapoints'])
            
            if max_memory_percent > memory_threshold:
                issues['high_memory_usage'].append({
                    'function_name': function_name,
                    'max_memory_percent': max_memory_percent,
                    'avg_memory_percent': avg_memory_percent,
                    'allocated_memory_mb': function_memory
                })
    
    # Send to SNS if configured
    if sns_topic_arn:
        sns = boto3.client('sns')
        
        # Count total issues
        total_issues = sum(len(issues[category]) for category in issues)
        
        if total_issues > 0:
            message = {
                'subject': f"Lambda Function Monitor - {total_issues} issues found",
                'timestamp': datetime.now().isoformat(),
                'issues': issues
            }
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(message, indent=2),
                Subject=message['subject']
            )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Lambda function monitoring completed',
            'issues': issues
        })
    }