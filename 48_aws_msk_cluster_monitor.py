import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Managed Streaming for Kafka (MSK) clusters.
    
    This function can:
    - Check the status of MSK clusters.
    - Monitor key CloudWatch metrics (e.g., CPU, disk usage, broker health).
    - Send notifications for unhealthy clusters or critical metric breaches.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - MSK_CLUSTER_ARNS: Comma-separated list of MSK Cluster ARNs to monitor.
    - SNS_TOPIC_ARN: SNS topic ARN for notifications of cluster issues.
    - CPU_UTILIZATION_THRESHOLD: Optional. Alert if average CPU util exceeds this (e.g., 80).
    - DISK_USAGE_THRESHOLD: Optional. Alert if average disk usage exceeds this (e.g., 85).
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    msk_cluster_arns_str = os.environ.get('MSK_CLUSTER_ARNS')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    # Optional thresholds for CloudWatch metrics
    # cpu_threshold = os.environ.get('CPU_UTILIZATION_THRESHOLD')
    # disk_threshold = os.environ.get('DISK_USAGE_THRESHOLD')

    if not msk_cluster_arns_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required environment variable: MSK_CLUSTER_ARNS')}
    if not sns_topic_arn:
        return {'statusCode': 400, 'body': json.dumps('Missing required environment variable: SNS_TOPIC_ARN')}

    msk_cluster_arns = [arn.strip() for arn in msk_cluster_arns_str.split(',')]

    # Initialize AWS clients
    msk_client = boto3.client('kafka', region_name=region)
    # cloudwatch_client = boto3.client('cloudwatch', region_name=region) # For metric checks
    sns = boto3.client('sns', region_name=region)
    
    monitor_results = {
        'timestamp': datetime.now().isoformat(),
        'problem_clusters': [],
        'checked_clusters_count': len(msk_cluster_arns),
        'errors': []
    }

    for cluster_arn in msk_cluster_arns:
        try:
            cluster_info = msk_client.describe_cluster_v2(ClusterArn=cluster_arn)['ClusterInfo']
            cluster_name = cluster_info.get('ClusterName', cluster_arn.split('/')[-1]) # Fallback to ARN part for name
            cluster_state = cluster_info.get('State')

            problem_details = []

            if cluster_state not in ['ACTIVE', 'UPDATING', 'MAINTENANCE', 'CREATING']: # Desired states
                problem_details.append(f"Cluster State is {cluster_state}")
            
            # Placeholder for CloudWatch metric checks (CPU, Disk, OfflinePartitionsCount, etc.)
            # Example:
            # if cpu_threshold:
            #   # Logic to get 'CpuUser' or 'CpuSystem' from CloudWatch for the cluster
            #   # If metric exceeds cpu_threshold, add to problem_details
            # if disk_threshold:
            #   # Logic to get 'KafkaDataLogsDiskUsed' from CloudWatch
            #   # If metric exceeds disk_threshold, add to problem_details

            if problem_details:
                problem_info = {
                    'ClusterArn': cluster_arn,
                    'ClusterName': cluster_name,
                    'Issues': ", ".join(problem_details)
                }
                monitor_results['problem_clusters'].append(problem_info)
                message = f"AWS MSK Cluster Alert for {cluster_name} (ARN: {cluster_arn}):\n"
                message += f"  Problems: {problem_info['Issues']}\n"
                message += f"  Check the AWS MSK console and CloudWatch metrics for details."
                sns.publish(TopicArn=sns_topic_arn, Message=message, Subject=f"MSK Cluster Issue: {cluster_name}")

        except msk_client.exceptions.NotFoundException:
            error_msg = f"MSK Cluster ARN {cluster_arn} not found."
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_clusters'].append({'ClusterArn': cluster_arn, 'Issue': 'Not Found'})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"MSK Cluster Issue: {cluster_arn} Not Found")
        except Exception as e_cluster:
            error_msg = f"Error processing MSK Cluster {cluster_arn}: {str(e_cluster)}"
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_clusters'].append({'ClusterArn': cluster_arn, 'Issue': str(e_cluster)})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"MSK Cluster Error: {cluster_arn.split('/')[-1]}")
            
    return {
        'statusCode': 200,
        'body': json.dumps(monitor_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['MSK_CLUSTER_ARNS'] = 'arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx-x' # Replace
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    # Note: MSK API calls require specific permissions.
    print(lambda_handler({}, {}))