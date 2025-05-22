import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor Elastic Beanstalk environments.
    
    This function monitors Elastic Beanstalk environments:
    - Checks environment health status
    - Monitors recent deployments
    - Analyzes environment events
    - Alerts on degraded environments
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ENVIRONMENT_NAMES: Optional comma-separated list of environment names to monitor
    - LOOKBACK_HOURS: Hours of history to analyze (default: 24)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    environment_names_str = os.environ.get('ENVIRONMENT_NAMES', '')
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    eb = boto3.client('elasticbeanstalk', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Parse environment names
    environment_names = [name.strip() for name in environment_names_str.split(',')] if environment_names_str else []
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'environments_checked': 0,
        'degraded_environments': [],
        'recent_deployments': [],
        'significant_events': []
    }
    
    # Get all environments or specified environments
    try:
        if environment_names:
            environments = []
            for env_name in environment_names:
                try:
                    response = eb.describe_environments(EnvironmentNames=[env_name])
                    environments.extend(response['Environments'])
                except Exception as e:
                    print(f"Error getting environment {env_name}: {str(e)}")
        else:
            response = eb.describe_environments()
            environments = response['Environments']
        
        results['environments_checked'] = len(environments)
        
        # Check each environment
        for env in environments:
            env_name = env['EnvironmentName']
            env_id = env['EnvironmentId']
            env_health = env['Health']
            env_health_status = env['HealthStatus']
            
            # Check if environment is degraded
            if env_health != 'Green':
                results['degraded_environments'].append({
                    'name': env_name,
                    'id': env_id,
                    'health': env_health,
                    'health_status': env_health_status,
                    'date_updated': env['DateUpdated'].isoformat() if 'DateUpdated' in env else None,
                    'abortable_operation_in_progress': env.get('AbortableOperationInProgress', False)
                })
            
            # Get recent deployments
            try:
                response = eb.describe_events(
                    EnvironmentName=env_name,
                    StartTime=start_time,
                    EndTime=end_time
                )
                
                # Filter for deployment events
                deployment_events = [
                    event for event in response['Events']
                    if 'deploy' in event['Message'].lower() or 'deployment' in event['Message'].lower()
                ]
                
                if deployment_events:
                    results['recent_deployments'].append({
                        'environment_name': env_name,
                        'deployments': [
                            {
                                'message': event['Message'],
                                'severity': event['Severity'],
                                'event_date': event['EventDate'].isoformat()
                            }
                            for event in deployment_events
                        ]
                    })
                
                # Filter for significant events (Warning or Error)
                significant_events = [
                    event for event in response['Events']
                    if event['Severity'] in ['WARN', 'ERROR']
                ]
                
                if significant_events:
                    results['significant_events'].append({
                        'environment_name': env_name,
                        'events': [
                            {
                                'message': event['Message'],
                                'severity': event['Severity'],
                                'event_date': event['EventDate'].isoformat()
                            }
                            for event in significant_events
                        ]
                    })
            except Exception as e:
                print(f"Error getting events for environment {env_name}: {str(e)}")
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error monitoring Elastic Beanstalk environments',
                'error': str(e)
            })
        }
    
    # Send notification if there are degraded environments and SNS topic is configured
    if results['degraded_environments'] and sns and sns_topic_arn:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Elastic Beanstalk Environment Alert - {end_time.strftime('%Y-%m-%d %H:%M')}",
                Message=json.dumps(results, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }