import boto3
import json
import os
import re
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to analyze VPC Flow Logs for security insights.
    
    This function queries VPC Flow Logs to identify:
    - Rejected traffic patterns
    - Unusual traffic volumes
    - Potential port scanning activity
    - Traffic to known malicious IPs
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - LOG_GROUP: CloudWatch Logs group containing VPC Flow Logs
    - ANALYSIS_PERIOD_HOURS: Hours of logs to analyze (default: 24)
    - REJECTION_THRESHOLD: Number of rejections to flag (default: 100)
    - PORT_SCAN_THRESHOLD: Number of distinct ports to consider as scanning (default: 15)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - SUSPICIOUS_IP_LIST: S3 URL to a list of suspicious IPs (optional)
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    log_group = os.environ.get('LOG_GROUP', '')
    analysis_period = int(os.environ.get('ANALYSIS_PERIOD_HOURS', 24))
    rejection_threshold = int(os.environ.get('REJECTION_THRESHOLD', 100))
    port_scan_threshold = int(os.environ.get('PORT_SCAN_THRESHOLD', 15))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    suspicious_ip_list = os.environ.get('SUSPICIOUS_IP_LIST', '')
    
    if not log_group:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable: LOG_GROUP')
        }
    
    # Initialize AWS clients
    logs = boto3.client('logs', region_name=region)
    s3 = boto3.client('s3')
    
    # Calculate time range for analysis
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=analysis_period)
    
    # Load suspicious IPs if provided
    suspicious_ips = set()
    if suspicious_ip_list:
        try:
            if suspicious_ip_list.startswith('s3://'):
                # Parse S3 URL
                s3_parts = suspicious_ip_list.replace('s3://', '').split('/')
                bucket = s3_parts[0]
                key = '/'.join(s3_parts[1:])
                
                response = s3.get_object(Bucket=bucket, Key=key)
                ip_content = response['Body'].read().decode('utf-8')
                
                # Extract IPs from content
                for line in ip_content.splitlines():
                    line = line.strip()
                    if line and not line.startswith('#'):
                        suspicious_ips.add(line)
        except Exception as e:
            print(f"Error loading suspicious IP list: {str(e)}")
    
    # Query for rejected traffic
    rejected_query = """
    filter action="REJECT"
    | stats count(*) as reject_count by srcAddr, dstAddr
    | filter reject_count > {}
    | sort reject_count desc
    | limit 100
    """.format(rejection_threshold)
    
    # Query for potential port scanning
    port_scan_query = """
    | stats count(distinct(dstPort)) as port_count by srcAddr
    | filter port_count > {}
    | sort port_count desc
    | limit 100
    """.format(port_scan_threshold)
    
    # Query for traffic to suspicious IPs
    suspicious_ip_query = ""
    if suspicious_ips:
        ip_list = '|'.join(suspicious_ips)
        suspicious_ip_query = f"""
        filter dstAddr matches /^({ip_list})$/
        | stats count(*) as hit_count by srcAddr, dstAddr
        | sort hit_count desc
        | limit 100
        """
    
    # Execute queries
    findings = {
        'rejected_traffic': execute_query(logs, log_group, rejected_query, start_time, end_time),
        'port_scanning': execute_query(logs, log_group, port_scan_query, start_time, end_time),
        'suspicious_ip_traffic': execute_query(logs, log_group, suspicious_ip_query, start_time, end_time) if suspicious_ip_query else []
    }
    
    # Send to SNS if configured
    if sns_topic_arn:
        sns = boto3.client('sns', region_name=region)
        
        # Count total findings
        total_findings = sum(len(findings[category]) for category in findings)
        
        if total_findings > 0:
            message = {
                'subject': f"VPC Flow Logs Analysis - {total_findings} security findings",
                'timestamp': datetime.now().isoformat(),
                'analysis_period': f"{analysis_period} hours",
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
            'message': 'VPC Flow Logs analysis completed',
            'analysis_period': f"{analysis_period} hours",
            'findings': findings
        })
    }

def execute_query(logs_client, log_group, query_string, start_time, end_time):
    """Execute a CloudWatch Logs Insights query and return results"""
    if not query_string.strip():
        return []
    
    try:
        # Start query
        start_query_response = logs_client.start_query(
            logGroupName=log_group,
            startTime=int(start_time.timestamp()),
            endTime=int(end_time.timestamp()),
            queryString=query_string
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
        results = []
        for result in response['results']:
            item = {}
            for field in result:
                item[field['field']] = field['value']
            results.append(item)
        
        return results
    
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return []