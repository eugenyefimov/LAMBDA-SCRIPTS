import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Database Migration Service (DMS) tasks.
    
    This function can:
    - Check the status of specified DMS replication tasks.
    - Report on tasks that are stopped, failed, or have high latency.
    - Send notifications for problematic tasks.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - DMS_TASK_ARNS: Comma-separated list of DMS Replication Task ARNs to monitor.
    - SNS_TOPIC_ARN: SNS topic ARN for notifications of task issues.
    - LATENCY_THRESHOLD_SECONDS: Optional. Alert if CDCLatencySource or CDCLatencyTarget exceeds this.
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    dms_task_arns_str = os.environ.get('DMS_TASK_ARNS')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    latency_threshold_str = os.environ.get('LATENCY_THRESHOLD_SECONDS')

    if not dms_task_arns_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: DMS_TASK_ARNS')}
    if not sns_topic_arn:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: SNS_TOPIC_ARN')}

    dms_task_arns = [arn.strip() for arn in dms_task_arns_str.split(',')]
    latency_threshold = int(latency_threshold_str) if latency_threshold_str else None

    # Initialize AWS clients
    dms_client = boto3.client('dms', region_name=region)
    sns = boto3.client('sns', region_name=region)
    
    monitor_results = {
        'timestamp': datetime.now().isoformat(),
        'problem_tasks': [],
        'checked_tasks_count': len(dms_task_arns),
        'errors': []
    }

    for task_arn in dms_task_arns:
        try:
            # Describe the replication task
            # To get all tasks, use describe_replication_tasks without filters and paginate.
            # For specific ARNs, it's better to filter if possible or describe one by one if API doesn't support batch ARN describe.
            # describe_replication_tasks uses Filters, not a direct list of ARNs.
            # So, we might need to list all and then filter, or if the list is small, describe one by one.
            # For this example, let's assume we describe tasks and then find the one matching ARN.
            # A more efficient way for many specific ARNs might be needed if the API allows.
            
            # Simpler: if we have task identifiers, we can use those. ARN is also an identifier.
            tasks_response = dms_client.describe_replication_tasks(
                Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
            )
            
            if not tasks_response.get('ReplicationTasks'):
                issue_msg = f"DMS Task ARN {task_arn} not found."
                monitor_results['errors'].append(issue_msg)
                monitor_results['problem_tasks'].append({'TaskArn': task_arn, 'Issue': 'Not Found'})
                sns.publish(TopicArn=sns_topic_arn, Message=issue_msg, Subject=f"DMS Task Issue: Not Found")
                continue

            task_info = tasks_response['ReplicationTasks'][0]
            task_id = task_info['ReplicationTaskIdentifier']
            task_status = task_info['Status']
            stop_reason = task_info.get('StopReason')
            stats = task_info.get('ReplicationTaskStats', {})
            cdc_latency_source = stats.get('CDCLatencySource')
            cdc_latency_target = stats.get('CDCLatencyTarget')

            problem_details = []

            # Check status
            # Desired states: 'running', 'starting', 'stopping', 'modifying', 'creating', 'testing'
            # Problematic states: 'stopped', 'failed', 'deleting'
            if task_status in ['stopped', 'failed']:
                problem_details.append(f"Task Status is {task_status}. Stop Reason: {stop_reason if stop_reason else 'N/A'}")
            
            # Check latency if threshold is set
            if latency_threshold is not None:
                if cdc_latency_source is not None and cdc_latency_source > latency_threshold:
                    problem_details.append(f"CDCLatencySource ({cdc_latency_source}s) exceeds threshold ({latency_threshold}s).")
                if cdc_latency_target is not None and cdc_latency_target > latency_threshold:
                    problem_details.append(f"CDCLatencyTarget ({cdc_latency_target}s) exceeds threshold ({latency_threshold}s).")

            if problem_details:
                problem_info = {
                    'TaskArn': task_arn,
                    'TaskId': task_id,
                    'CurrentStatus': task_status,
                    'Issues': ", ".join(problem_details)
                }
                monitor_results['problem_tasks'].append(problem_info)
                message = f"AWS DMS Task Alert for {task_id} (ARN: {task_arn}):\n"
                message += f"  Problems: {problem_info['Issues']}\n"
                message += f"  Check the AWS DMS console for details."
                sns.publish(TopicArn=sns_topic_arn, Message=message, Subject=f"DMS Task Issue: {task_id}")

        except Exception as e_task:
            error_msg = f"Error processing DMS Task {task_arn}: {str(e_task)}"
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_tasks'].append({'TaskArn': task_arn, 'Issue': str(e_task)})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"DMS Task Monitor Error")
            
    return {
        'statusCode': 200,
        'body': json.dumps(monitor_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    # You'll need an actual DMS task ARN for this to work.
    os.environ['DMS_TASK_ARNS'] = 'arn:aws:dms:us-east-1:123456789012:task:YOURTASKIDENTIFIER' # Replace
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    # os.environ['LATENCY_THRESHOLD_SECONDS'] = '300'
    print(lambda_handler({}, {}))