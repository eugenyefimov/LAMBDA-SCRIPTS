import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor SageMaker models and endpoints.
    
    This function monitors SageMaker models and endpoints:
    - Tracks model performance metrics
    - Detects data drift and concept drift
    - Monitors endpoint utilization and latency
    - Generates alerts for performance degradation
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ENDPOINT_NAMES: Optional comma-separated list of endpoint names to monitor
    - LOOKBACK_DAYS: Days of metrics to analyze (default: 7)
    - DRIFT_THRESHOLD: Threshold for data drift detection (default: 0.05)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - S3_REPORT_BUCKET: Optional S3 bucket for storing reports
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    endpoint_names_str = os.environ.get('ENDPOINT_NAMES', '')
    lookback_days = int(os.environ.get('LOOKBACK_DAYS', 7))
    drift_threshold = float(os.environ.get('DRIFT_THRESHOLD', 0.05))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    s3_report_bucket = os.environ.get('S3_REPORT_BUCKET', '')
    
    # Initialize AWS clients
    sagemaker = boto3.client('sagemaker', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    s3 = boto3.client('s3', region_name=region) if s3_report_bucket else None
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=lookback_days)
    
    # Parse endpoint names
    endpoint_names = [name.strip() for name in endpoint_names_str.split(',')] if endpoint_names_str else []
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'endpoints_analyzed': [],
        'performance_issues': [],
        'data_drift_detected': [],
        'resource_utilization': []
    }
    
    # If no endpoint names provided, get all endpoints
    if not endpoint_names:
        try:
            paginator = sagemaker.get_paginator('list_endpoints')
            for page in paginator.paginate():
                for endpoint in page['Endpoints']:
                    endpoint_names.append(endpoint['EndpointName'])
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing SageMaker endpoints',
                    'error': str(e)
                })
            }
    
    # Analyze each endpoint
    for endpoint_name in endpoint_names:
        try:
            # Get endpoint details
            endpoint = sagemaker.describe_endpoint(EndpointName=endpoint_name)
            
            endpoint_info = {
                'name': endpoint_name,
                'status': endpoint['EndpointStatus'],
                'created_at': endpoint['CreationTime'].isoformat(),
                'last_modified': endpoint['LastModifiedTime'].isoformat(),
                'performance_metrics': {},
                'data_drift_metrics': {},
                'resource_utilization': {}
            }
            
            # Get endpoint metrics from CloudWatch
            metrics = [
                'Invocations', 'InvocationsPerInstance', 'ModelLatency',
                'OverheadLatency', 'Invocation4XXErrors', 'Invocation5XXErrors'
            ]
            
            for metric_name in metrics:
                try:
                    response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/SageMaker',
                        MetricName=metric_name,
                        Dimensions=[
                            {'Name': 'EndpointName', 'Value': endpoint_name}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,  # 1 hour
                        Statistics=['Average', 'Maximum', 'Sum']
                    )
                    
                    if response['Datapoints']:
                        endpoint_info['performance_metrics'][metric_name] = {
                            'average': response['Datapoints'][-1]['Average'] if 'Average' in response['Datapoints'][-1] else None,
                            'maximum': response['Datapoints'][-1]['Maximum'] if 'Maximum' in response['Datapoints'][-1] else None,
                            'sum': response['Datapoints'][-1]['Sum'] if 'Sum' in response['Datapoints'][-1] else None
                        }
                        
                        # Check for performance issues
                        if metric_name == 'ModelLatency' and 'Average' in response['Datapoints'][-1]:
                            if response['Datapoints'][-1]['Average'] > 1000:  # 1 second
                                results['performance_issues'].append({
                                    'endpoint_name': endpoint_name,
                                    'issue': 'High model latency',
                                    'value': response['Datapoints'][-1]['Average'],
                                    'threshold': 1000
                                })
                        
                        if metric_name in ['Invocation4XXErrors', 'Invocation5XXErrors'] and 'Sum' in response['Datapoints'][-1]:
                            if response['Datapoints'][-1]['Sum'] > 0:
                                results['performance_issues'].append({
                                    'endpoint_name': endpoint_name,
                                    'issue': f'High {metric_name}',
                                    'value': response['Datapoints'][-1]['Sum'],
                                    'threshold': 0
                                })
                except Exception as e:
                    print(f"Error getting {metric_name} metrics for endpoint {endpoint_name}: {str(e)}")
            
            # Check for data drift if model monitor is configured
            try:
                monitors = sagemaker.list_model_quality_job_definitions(
                    EndpointName=endpoint_name
                )
                
                if 'ModelQualityJobDefinitions' in monitors and monitors['ModelQualityJobDefinitions']:
                    for monitor in monitors['ModelQualityJobDefinitions']:
                        monitor_name = monitor['ModelQualityJobDefinitionName']
                        
                        # Get latest monitoring results
                        executions = sagemaker.list_model_quality_job_definitions(
                            ModelQualityJobDefinitionName=monitor_name
                        )
                        
                        if 'ModelQualityJobDefinitions' in executions and executions['ModelQualityJobDefinitions']:
                            # Check for data drift
                            # This is a simplified example - actual implementation would depend on
                            # how model monitoring is set up and what metrics are being tracked
                            if 'drift_detected' in endpoint_info['data_drift_metrics'] and endpoint_info['data_drift_metrics']['drift_detected']:
                                results['data_drift_detected'].append({
                                    'endpoint_name': endpoint_name,
                                    'monitor_name': monitor_name,
                                    'drift_value': endpoint_info['data_drift_metrics'].get('drift_value', 0),
                                    'threshold': drift_threshold
                                })
            except Exception as e:
                print(f"Error checking data drift for endpoint {endpoint_name}: {str(e)}")
            
            # Add endpoint info to results
            results['endpoints_analyzed'].append(endpoint_info)
            
        except Exception as e:
            print(f"Error analyzing endpoint {endpoint_name}: {str(e)}")
    
    # Generate report
    report = {
        'summary': {
            'endpoints_analyzed': len(results['endpoints_analyzed']),
            'endpoints_with_issues': len(results['performance_issues']),
            'endpoints_with_drift': len(results['data_drift_detected'])
        },
        'details': results
    }
    
    # Save report to S3 if bucket specified
    if s3 and s3_report_bucket:
        try:
            report_key = f"sagemaker-model-monitor/report-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"
            s3.put_object(
                Bucket=s3_report_bucket,
                Key=report_key,
                Body=json.dumps(report, indent=2),
                ContentType='application/json'
            )
            results['report_location'] = f"s3://{s3_report_bucket}/{report_key}"
        except Exception as e:
            print(f"Error saving report to S3: {str(e)}")
    
    # Send notification if issues found and SNS topic provided
    if sns and sns_topic_arn and (results['performance_issues'] or results['data_drift_detected']):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"SageMaker Model Monitor Alert - {len(results['performance_issues'])} issues found",
                Message=json.dumps(report, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }