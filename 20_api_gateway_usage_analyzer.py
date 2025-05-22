import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to analyze API Gateway usage patterns.
    
    This function analyzes API Gateway metrics and logs to identify:
    - Most frequently used endpoints
    - Endpoints with high error rates
    - Latency issues
    - Usage patterns by client/IP
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - API_IDS: Comma-separated list of API Gateway IDs to analyze
    - DAYS_TO_ANALYZE: Number of days of data to analyze (default: 7)
    - ERROR_THRESHOLD: Error rate threshold percentage (default: 5)
    - LATENCY_THRESHOLD_MS: Latency threshold in milliseconds (default: 1000)
    - S3_REPORT_BUCKET: Optional S3 bucket for storing reports
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    api_ids_str = os.environ.get('API_IDS', '')
    days_to_analyze = int(os.environ.get('DAYS_TO_ANALYZE', 7))
    error_threshold = float(os.environ.get('ERROR_THRESHOLD', 5))
    latency_threshold = int(os.environ.get('LATENCY_THRESHOLD_MS', 1000))
    s3_report_bucket = os.environ.get('S3_REPORT_BUCKET', '')
    
    # Initialize AWS clients
    apigw = boto3.client('apigateway', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    logs = boto3.client('logs', region_name=region)
    s3 = boto3.client('s3', region_name=region) if s3_report_bucket else None
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_to_analyze)
    
    # Parse API IDs
    api_ids = [api_id.strip() for api_id in api_ids_str.split(',')] if api_ids_str else []
    
    # If no API IDs provided, get all APIs
    if not api_ids:
        try:
            response = apigw.get_rest_apis()
            api_ids = [api['id'] for api in response['items']]
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing API Gateway APIs',
                    'error': str(e)
                })
            }
    
    results = {
        'analyzed_apis': [],
        'high_traffic_endpoints': [],
        'high_error_endpoints': [],
        'high_latency_endpoints': []
    }
    
    # Analyze each API
    for api_id in api_ids:
        api_result = {
            'api_id': api_id,
            'endpoints_analyzed': 0,
            'total_requests': 0,
            'error_count': 0,
            'avg_latency': 0
        }
        
        try:
            # Get API details
            api_details = apigw.get_rest_api(restApiId=api_id)
            api_name = api_details.get('name', 'Unknown')
            api_result['api_name'] = api_name
            
            # Get resources/endpoints
            resources = apigw.get_resources(restApiId=api_id)
            
            for resource in resources['items']:
                resource_path = resource.get('path', '/')
                resource_methods = resource.get('resourceMethods', {})
                
                for method in resource_methods:
                    endpoint = f"{method}:{resource_path}"
                    api_result['endpoints_analyzed'] += 1
                    
                    # Get request count
                    count_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ApiGateway',
                        MetricName='Count',
                        Dimensions=[
                            {'Name': 'ApiId', 'Value': api_id},
                            {'Name': 'Resource', 'Value': resource_path},
                            {'Name': 'Method', 'Value': method},
                            {'Name': 'Stage', 'Value': 'prod'}  # Assuming 'prod' stage, adjust as needed
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,  # Daily
                        Statistics=['Sum']
                    )
                    
                    request_count = sum(point['Sum'] for point in count_response.get('Datapoints', []))
                    api_result['total_requests'] += request_count
                    
                    # Get error count (4xx and 5xx)
                    error_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ApiGateway',
                        MetricName='4XXError',
                        Dimensions=[
                            {'Name': 'ApiId', 'Value': api_id},
                            {'Name': 'Resource', 'Value': resource_path},
                            {'Name': 'Method', 'Value': method},
                            {'Name': 'Stage', 'Value': 'prod'}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Sum']
                    )
                    
                    error_4xx = sum(point['Sum'] for point in error_response.get('Datapoints', []))
                    
                    error_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ApiGateway',
                        MetricName='5XXError',
                        Dimensions=[
                            {'Name': 'ApiId', 'Value': api_id},
                            {'Name': 'Resource', 'Value': resource_path},
                            {'Name': 'Method', 'Value': method},
                            {'Name': 'Stage', 'Value': 'prod'}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Sum']
                    )
                    
                    error_5xx = sum(point['Sum'] for point in error_response.get('Datapoints', []))
                    total_errors = error_4xx + error_5xx
                    api_result['error_count'] += total_errors
                    
                    # Get latency
                    latency_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ApiGateway',
                        MetricName='Latency',
                        Dimensions=[
                            {'Name': 'ApiId', 'Value': api_id},
                            {'Name': 'Resource', 'Value': resource_path},
                            {'Name': 'Method', 'Value': method},
                            {'Name': 'Stage', 'Value': 'prod'}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Average']
                    )
                    
                    avg_latency = 0
                    if latency_response.get('Datapoints'):
                        avg_latency = sum(point['Average'] for point in latency_response['Datapoints']) / len(latency_response['Datapoints'])
                    
                    # Check for high traffic
                    if request_count > 0:
                        # Add to high traffic if in top 10% of endpoints
                        results['high_traffic_endpoints'].append({
                            'api_id': api_id,
                            'api_name': api_name,
                            'endpoint': endpoint,
                            'request_count': request_count
                        })
                    
                    # Check for high error rate
                    if request_count > 0 and (total_errors / request_count * 100) > error_threshold:
                        results['high_error_endpoints'].append({
                            'api_id': api_id,
                            'api_name': api_name,
                            'endpoint': endpoint,
                            'error_rate': (total_errors / request_count * 100),
                            'request_count': request_count,
                            'error_count': total_errors
                        })
                    
                    # Check for high latency
                    if avg_latency > latency_threshold:
                        results['high_latency_endpoints'].append({
                            'api_id': api_id,
                            'api_name': api_name,
                            'endpoint': endpoint,
                            'avg_latency_ms': avg_latency,
                            'request_count': request_count
                        })
            
            # Calculate overall API latency
            latency_response = cloudwatch.get_metric_statistics(
                Namespace='AWS/ApiGateway',
                MetricName='Latency',
                Dimensions=[
                    {'Name': 'ApiId', 'Value': api_id}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,
                Statistics=['Average']
            )
            
            if latency_response.get('Datapoints'):
                api_result['avg_latency'] = sum(point['Average'] for point in latency_response['Datapoints']) / len(latency_response['Datapoints'])
            
            results['analyzed_apis'].append(api_result)
            
        except Exception as e:
            results['analyzed_apis'].append({
                'api_id': api_id,
                'error': str(e)
            })
    
    # Sort high traffic endpoints
    results['high_traffic_endpoints'] = sorted(
        results['high_traffic_endpoints'],
        key=lambda x: x['request_count'],
        reverse=True
    )[:10]  # Keep only top 10
    
    # Generate report
    if s3_report_bucket:
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'time_range': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat()
                },
                'results': results
            }
            
            s3.put_object(
                Bucket=s3_report_bucket,
                Key=f"api-gateway-reports/{end_time.strftime('%Y-%m-%d')}.json",
                Body=json.dumps(report, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Error saving report to S3: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'API Gateway usage analysis completed',
            'time_range': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            },
            'results': results
        })
    }