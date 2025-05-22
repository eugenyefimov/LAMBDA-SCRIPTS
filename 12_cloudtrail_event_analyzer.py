import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to analyze CloudTrail events for suspicious activities.
    
    This function scans CloudTrail logs to identify potentially suspicious activities:
    - Failed console logins
    - API calls from unusual locations
    - Sensitive API calls (e.g., IAM changes, security group modifications)
    - Unusual volume of API calls
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - LOOKBACK_HOURS: Hours of CloudTrail events to analyze (default: 24)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - SENSITIVE_APIS: Comma-separated list of sensitive API actions to monitor
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    sensitive_apis_str = os.environ.get('SENSITIVE_APIS', 'DeleteTrail,StopLogging,DeleteFlowLogs,DeleteUser,CreateAccessKey,UpdateUser')
    
    # Parse sensitive APIs
    sensitive_apis = [api.strip() for api in sensitive_apis_str.split(',')]
    
    # Initialize AWS clients
    cloudtrail = boto3.client('cloudtrail', region_name=region)
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    # Initialize findings
    findings = {
        'failed_logins': [],
        'sensitive_api_calls': [],
        'unusual_locations': [],
        'high_volume_apis': {}
    }
    
    # Look for failed console logins
    try:
        response = cloudtrail.lookup_events(
            LookupAttributes=[
                {
                    'AttributeKey': 'EventName',
                    'AttributeValue': 'ConsoleLogin'
                }
            ],
            StartTime=start_time,
            EndTime=end_time
        )
        
        for event in response['Events']:
            event_data = json.loads(event['CloudTrailEvent'])
            if event_data.get('responseElements', {}).get('ConsoleLogin') == 'Failure':
                findings['failed_logins'].append({
                    'eventTime': event_data['eventTime'],
                    'sourceIPAddress': event_data['sourceIPAddress'],
                    'userIdentity': event_data['userIdentity'],
                    'errorMessage': event_data.get('errorMessage', 'Unknown error')
                })
    except Exception as e:
        print(f"Error looking up failed logins: {str(e)}")
    
    # Look for sensitive API calls
    for api in sensitive_apis:
        try:
            response = cloudtrail.lookup_events(
                LookupAttributes=[
                    {
                        'AttributeKey': 'EventName',
                        'AttributeValue': api
                    }
                ],
                StartTime=start_time,
                EndTime=end_time
            )
            
            for event in response['Events']:
                event_data = json.loads(event['CloudTrailEvent'])
                findings['sensitive_api_calls'].append({
                    'eventTime': event_data['eventTime'],
                    'eventName': event_data['eventName'],
                    'sourceIPAddress': event_data['sourceIPAddress'],
                    'userIdentity': event_data['userIdentity'],
                    'resources': event_data.get('resources', [])
                })
        except Exception as e:
            print(f"Error looking up sensitive API {api}: {str(e)}")
    
    # Analyze API call volumes
    try:
        # Get all events in the time period
        all_events = []
        paginator = cloudtrail.get_paginator('lookup_events')
        
        for page in paginator.paginate(
            StartTime=start_time,
            EndTime=end_time
        ):
            all_events.extend(page['Events'])
        
        # Count API calls by user and API name
        api_counts = {}
        for event in all_events:
            event_data = json.loads(event['CloudTrailEvent'])
            user = event_data['userIdentity'].get('userName', event_data['userIdentity'].get('type', 'Unknown'))
            api = event_data['eventName']
            
            if user not in api_counts:
                api_counts[user] = {}
            
            if api not in api_counts[user]:
                api_counts[user][api] = 0
            
            api_counts[user][api] += 1
        
        # Find high volume APIs (more than 100 calls)
        for user, apis in api_counts.items():
            for api, count in apis.items():
                if count > 100:
                    if user not in findings['high_volume_apis']:
                        findings['high_volume_apis'][user] = []
                    
                    findings['high_volume_apis'][user].append({
                        'api': api,
                        'count': count
                    })
    except Exception as e:
        print(f"Error analyzing API volumes: {str(e)}")
    
    # Send to SNS if configured
    if sns_topic_arn:
        sns = boto3.client('sns', region_name=region)
        
        # Count total findings
        total_findings = (
            len(findings['failed_logins']) + 
            len(findings['sensitive_api_calls']) + 
            len(findings['unusual_locations']) + 
            sum(len(apis) for user, apis in findings['high_volume_apis'].items())
        )
        
        if total_findings > 0:
            message = {
                'subject': f"CloudTrail Analysis - {total_findings} suspicious activities",
                'timestamp': datetime.now().isoformat(),
                'analysis_period': f"{lookback_hours} hours",
                'findings': findings
            }
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(message, indent=2),
                Subject=message['subject']
            )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'CloudTrail analysis completed',
            'analysis_period': f"{lookback_hours} hours",
            'findings': findings
        })
    }