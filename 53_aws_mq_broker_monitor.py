import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS MQ (Amazon MQ) brokers.
    
    This function can:
    - Check the status of specified MQ brokers.
    - Monitor key CloudWatch metrics (e.g., CPU, memory, storage, queue depth).
    - Send notifications for unhealthy brokers or critical metric breaches.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - MQ_BROKER_IDS: Comma-separated list of MQ Broker IDs to monitor.
    - SNS_TOPIC_ARN: SNS topic ARN for notifications of broker issues.
    - CPU_UTILIZATION_THRESHOLD: Optional. Alert if CPU utilization exceeds this (e.g., 80).
    - MEMORY_USAGE_THRESHOLD: Optional. Alert if memory usage exceeds this (e.g., 85).
    - STORAGE_PERCENT_USAGE_THRESHOLD: Optional. Alert if storage usage exceeds this (e.g., 85).
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    mq_broker_ids_str = os.environ.get('MQ_BROKER_IDS')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    # Optional thresholds for CloudWatch metrics
    # cpu_threshold = os.environ.get('CPU_UTILIZATION_THRESHOLD')
    # memory_threshold = os.environ.get('MEMORY_USAGE_THRESHOLD')
    # storage_threshold = os.environ.get('STORAGE_PERCENT_USAGE_THRESHOLD')

    if not mq_broker_ids_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: MQ_BROKER_IDS')}
    if not sns_topic_arn:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: SNS_TOPIC_ARN')}

    mq_broker_ids = [bid.strip() for bid in mq_broker_ids_str.split(',')]

    # Initialize AWS clients
    mq_client = boto3.client('mq', region_name=region)
    # cloudwatch_client = boto3.client('cloudwatch', region_name=region) # For metric checks
    sns = boto3.client('sns', region_name=region)
    
    monitor_results = {
        'timestamp': datetime.now().isoformat(),
        'problem_brokers': [],
        'checked_brokers_count': len(mq_broker_ids),
        'errors': []
    }

    for broker_id in mq_broker_ids:
        try:
            broker_info = mq_client.describe_broker(BrokerId=broker_id)
            broker_name = broker_info.get('BrokerName', broker_id)
            broker_state = broker_info.get('BrokerState') # e.g., RUNNING, REBOOTING, DELETION_IN_PROGRESS
            # Health can also be inferred from BrokerInstances' ConsoleURL availability or specific health metrics

            problem_details = []

            if broker_state not in ['RUNNING']:
                problem_details.append(f"Broker State is {broker_state}")
            
            # Placeholder for CloudWatch metric checks (CpuUtilization, MemoryUsage, StoragePercentUsage, TotalMessageCount etc.)
            # Example:
            # if cpu_threshold:
            #   # Logic to get 'CpuUtilization' from CloudWatch for the broker
            #   # If metric exceeds cpu_threshold, add to problem_details
            # if memory_threshold:
            #   # Logic to get 'MemoryUsage' from CloudWatch
            #   # If metric exceeds memory_threshold, add to problem_details
            # if storage_threshold:
            #   # Logic to get 'StoragePercentUsage' from CloudWatch
            #   # If metric exceeds storage_threshold, add to problem_details

            if problem_details:
                problem_info = {
                    'BrokerId': broker_id,
                    'BrokerName': broker_name,
                    'CurrentState': broker_state,
                    'Issues': ", ".join(problem_details)
                }
                monitor_results['problem_brokers'].append(problem_info)
                message = f"AWS MQ Broker Alert for {broker_name} (ID: {broker_id}):\n"
                message += f"  Problems: {problem_info['Issues']}\n"
                message += f"  Check the AWS MQ console and CloudWatch metrics for details."
                sns.publish(TopicArn=sns_topic_arn, Message=message, Subject=f"MQ Broker Issue: {broker_name}")

        except mq_client.exceptions.NotFoundException:
            error_msg = f"MQ Broker ID {broker_id} not found."
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_brokers'].append({'BrokerId': broker_id, 'Issue': 'Not Found'})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"MQ Broker Issue: {broker_id} Not Found")
        except Exception as e_broker:
            error_msg = f"Error processing MQ Broker {broker_id}: {str(e_broker)}"
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_brokers'].append({'BrokerId': broker_id, 'Issue': str(e_broker)})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"MQ Broker Error: {broker_id}")
            
    return {
        'statusCode': 200,
        'body': json.dumps(monitor_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    # You'll need an actual MQ Broker ID for this to work.
    os.environ['MQ_BROKER_IDS'] = 'b-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' # Replace with your Broker ID
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    # os.environ['CPU_UTILIZATION_THRESHOLD'] = '80'
    print(lambda_handler({}, {}))