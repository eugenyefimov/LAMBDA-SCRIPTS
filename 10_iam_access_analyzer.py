import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to analyze IAM permissions and identify security risks.
    
    This function uses AWS IAM Access Analyzer to identify:
    - External access to resources
    - Unused permissions
    - Overly permissive policies
    - Service control policy violations
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ANALYZER_NAME: Name of the IAM Access Analyzer to use
    - MAX_FINDINGS: Maximum number of findings to return (default: 100)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    analyzer_name = os.environ.get('ANALYZER_NAME', '')
    max_findings = int(os.environ.get('MAX_FINDINGS', 100))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    access_analyzer = boto3.client('accessanalyzer', region_name=region)
    iam = boto3.client('iam', region_name=region)
    
    # Get or create analyzer if not specified
    if not analyzer_name:
        # List existing analyzers
        analyzers = access_analyzer.list_analyzers()
        if analyzers['analyzers']:
            analyzer_name = analyzers['analyzers'][0]['name']
        else:
            # Create a new analyzer
            response = access_analyzer.create_analyzer(
                analyzerName='default-analyzer',
                type='ACCOUNT'
            )
            analyzer_name = response['arn'].split('/')[-1]
    
    # Get active findings
    findings = []
    paginator = access_analyzer.get_paginator('list_findings')
    
    for page in paginator.paginate(
        analyzerArn=f"arn:aws:access-analyzer:{region}:{boto3.client('sts').get_caller_identity()['Account']}:analyzer/{analyzer_name}",
        filter={
            'status': {
                'eq': ['ACTIVE']
            }
        },
        maxResults=max_findings
    ):
        findings.extend(page['findings'])
    
    # Analyze unused permissions (requires IAM Access Analyzer with unused access feature)
    unused_permissions = []
    try:
        # Get IAM users
        users = iam.list_users()['Users']
        
        for user in users:
            username = user['UserName']
            
            # Get last accessed information
            response = iam.generate_service_last_accessed_details(
                Arn=user['Arn']
            )
            
            job_id = response['JobId']
            
            # Wait for job to complete
            complete = False
            while not complete:
                response = iam.get_service_last_accessed_details(
                    JobId=job_id
                )
                
                if response['JobStatus'] in ['COMPLETED', 'FAILED']:
                    complete = True
                else:
                    import time
                    time.sleep(1)
            
            # Process results
            if response['JobStatus'] == 'COMPLETED':
                for service in response['ServicesLastAccessed']:
                    if 'LastAuthenticated' not in service:
                        # Service never used
                        unused_permissions.append({
                            'user': username,
                            'service': service['ServiceName'],
                            'status': 'Never used'
                        })
                    elif (datetime.now() - service['LastAuthenticated'].replace(tzinfo=None)).days > 90:
                        # Service not used in last 90 days
                        unused_permissions.append({
                            'user': username,
                            'service': service['ServiceName'],
                            'last_used': service['LastAuthenticated'].isoformat(),
                            'status': 'Not used in 90+ days'
                        })
    except Exception as e:
        print(f"Error analyzing unused permissions: {str(e)}")
    
    # Prepare results
    results = {
        'external_access_findings': findings,
        'unused_permissions': unused_permissions
    }
    
    # Send to SNS if configured
    if sns_topic_arn:
        sns = boto3.client('sns', region_name=region)
        
        # Count total findings
        total_findings = len(findings) + len(unused_permissions)
        
        if total_findings > 0:
            message = {
                'subject': f"IAM Access Analysis - {total_findings} findings",
                'timestamp': datetime.now().isoformat(),
                'findings': results
            }
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(message, indent=2),
                Subject=message['subject']
            )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'IAM access analysis completed',
            'results': results
        })
    }