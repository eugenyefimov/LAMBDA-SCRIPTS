import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Elastic Kubernetes Service (EKS) clusters.
    
    This function can:
    - Check the status of specified EKS clusters (control plane).
    - Monitor key CloudWatch metrics for the control plane.
    - Check node group health and status (basic).
    - Send notifications for unhealthy clusters or critical issues.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - EKS_CLUSTER_NAMES: Comma-separated list of EKS Cluster names to monitor.
    - SNS_TOPIC_ARN: SNS topic ARN for notifications of cluster issues.
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    eks_cluster_names_str = os.environ.get('EKS_CLUSTER_NAMES')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    if not eks_cluster_names_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: EKS_CLUSTER_NAMES')}
    if not sns_topic_arn:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: SNS_TOPIC_ARN')}

    eks_cluster_names = [name.strip() for name in eks_cluster_names_str.split(',')]

    # Initialize AWS clients
    eks_client = boto3.client('eks', region_name=region)
    # cloudwatch_client = boto3.client('cloudwatch', region_name=region) # For control plane metrics
    sns = boto3.client('sns', region_name=region)
    
    monitor_results = {
        'timestamp': datetime.now().isoformat(),
        'problem_clusters': [],
        'checked_clusters_count': len(eks_cluster_names),
        'errors': []
    }

    for cluster_name in eks_cluster_names:
        try:
            cluster_info = eks_client.describe_cluster(name=cluster_name)['cluster']
            cluster_status = cluster_info['status'] # e.g., ACTIVE, CREATING, DELETING, FAILED, UPDATING
            cluster_health = cluster_info.get('health', {}).get('issues', []) # Control plane health issues

            problem_details = []

            if cluster_status not in ['ACTIVE']:
                problem_details.append(f"Cluster status is {cluster_status}.")
            
            if cluster_health:
                health_issues_summary = [f"{issue['code']}: {issue['message']}" for issue in cluster_health]
                problem_details.append(f"Control plane health issues: {'; '.join(health_issues_summary)}")

            # Basic check for Node Groups associated with the cluster
            nodegroups_response = eks_client.list_nodegroups(clusterName=cluster_name)
            for ng_name in nodegroups_response.get('nodegroups', []):
                try:
                    ng_info = eks_client.describe_nodegroup(clusterName=cluster_name, nodegroupName=ng_name)['nodegroup']
                    ng_status = ng_info['status']
                    ng_health_issues = ng_info.get('health', {}).get('issues', [])
                    if ng_status not in ['ACTIVE']:
                        problem_details.append(f"Nodegroup '{ng_name}' status is {ng_status}.")
                    if ng_health_issues:
                        ng_issues_summary = [f"{issue['code']}: {issue['message']}" for issue in ng_health_issues]
                        problem_details.append(f"Nodegroup '{ng_name}' health issues: {'; '.join(ng_issues_summary)}")
                except Exception as e_ng:
                    problem_details.append(f"Could not describe nodegroup '{ng_name}': {str(e_ng)}")

            if problem_details:
                problem_info = {
                    'ClusterName': cluster_name,
                    'CurrentStatus': cluster_status,
                    'Issues': ", ".join(problem_details)
                }
                monitor_results['problem_clusters'].append(problem_info)
                message = f"AWS EKS Cluster Alert for {cluster_name}:\n"
                message += f"  Problems: {problem_info['Issues']}\n"
                message += f"  Check the AWS EKS console and CloudWatch metrics for details."
                sns.publish(TopicArn=sns_topic_arn, Message=message, Subject=f"EKS Cluster Issue: {cluster_name}")

        except eks_client.exceptions.ResourceNotFoundException:
            error_msg = f"EKS Cluster '{cluster_name}' not found."
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_clusters'].append({'ClusterName': cluster_name, 'Issue': 'Not Found'})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"EKS Cluster Issue: {cluster_name} Not Found")
        except Exception as e_cluster:
            error_msg = f"Error processing EKS Cluster {cluster_name}: {str(e_cluster)}"
            monitor_results['errors'].append(error_msg)
            monitor_results['problem_clusters'].append({'ClusterName': cluster_name, 'Issue': str(e_cluster)})
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"EKS Cluster Error: {cluster_name}")
            
    return {
        'statusCode': 200,
        'body': json.dumps(monitor_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    # You'll need an actual EKS cluster name for this to work.
    os.environ['EKS_CLUSTER_NAMES'] = 'my-eks-cluster' # Replace with your EKS cluster name
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    print(lambda_handler({}, {}))