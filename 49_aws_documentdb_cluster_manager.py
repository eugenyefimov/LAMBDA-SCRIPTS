import boto3
import json
import os
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    """
    AWS Lambda function to manage AWS DocumentDB clusters.
    
    This function can:
    - Create manual snapshots for specified DocumentDB clusters.
    - Delete old manual snapshots based on a retention policy.
    - Optionally, initiate maintenance or check cluster status.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - DOCDB_CLUSTER_IDENTIFIERS: Comma-separated list of DocumentDB Cluster Identifiers.
    - RETENTION_DAYS: Number of days to retain manual snapshots (e.g., 7, 30).
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications.
    - ACTION: 'backup' or 'cleanup' or 'all' (default: 'all')
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    docdb_cluster_ids_str = os.environ.get('DOCDB_CLUSTER_IDENTIFIERS')
    retention_days_str = os.environ.get('RETENTION_DAYS')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    action = os.environ.get('ACTION', 'all').lower()

    if not docdb_cluster_ids_str and action in ['backup', 'all']:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: DOCDB_CLUSTER_IDENTIFIERS for backup action')}
    if not retention_days_str and action in ['cleanup', 'all']:
        return {'statusCode': 400, 'body': json.dumps('Missing required env var: RETENTION_DAYS for cleanup action')}

    docdb_cluster_ids = [cid.strip() for cid in docdb_cluster_ids_str.split(',')] if docdb_cluster_ids_str else []
    
    try:
        retention_days = int(retention_days_str) if retention_days_str else 0
    except ValueError:
        return {'statusCode': 400, 'body': json.dumps('Invalid RETENTION_DAYS. Must be an integer.')}

    # Initialize AWS clients
    docdb_client = boto3.client('docdb', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'snapshots_created': [],
        'snapshots_deleted': [],
        'errors': []
    }

    # Create snapshots
    if action in ['backup', 'all']:
        for cluster_id in docdb_cluster_ids:
            try:
                snapshot_identifier = f"{cluster_id}-manual-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M-%S')}"
                response = docdb_client.create_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier=snapshot_identifier,
                    DBClusterIdentifier=cluster_id
                )
                snapshot_info = response.get('DBClusterSnapshot', {})
                results['snapshots_created'].append({
                    'ClusterIdentifier': cluster_id,
                    'SnapshotIdentifier': snapshot_info.get('DBClusterSnapshotIdentifier'),
                    'Status': snapshot_info.get('Status')
                })
                if sns:
                    sns.publish(
                        TopicArn=sns_topic_arn,
                        Message=f"Successfully initiated DocumentDB snapshot {snapshot_identifier} for cluster {cluster_id}.",
                        Subject=f"DocumentDB Snapshot Created: {cluster_id}"
                    )
            except Exception as e:
                error_msg = f"Error creating snapshot for DocumentDB cluster {cluster_id}: {str(e)}"
                results['errors'].append(error_msg)
                if sns:
                    sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"DocumentDB Snapshot Creation Error: {cluster_id}")

    # Delete old manual snapshots
    if action in ['cleanup', 'all'] and retention_days > 0:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        try:
            paginator = docdb_client.get_paginator('describe_db_cluster_snapshots')
            for page in paginator.paginate(SnapshotType='manual'): # Only manage manual snapshots
                for snapshot in page.get('DBClusterSnapshots', []):
                    snapshot_id = snapshot['DBClusterSnapshotIdentifier']
                    cluster_id_of_snapshot = snapshot.get('DBClusterIdentifier') # For filtering if needed
                    
                    # Optional: Only delete snapshots for the clusters managed by this lambda
                    # if docdb_cluster_ids and cluster_id_of_snapshot not in docdb_cluster_ids:
                    #     continue

                    snapshot_create_time = snapshot['SnapshotCreateTime'] # Already timezone-aware (UTC)
                    
                    if snapshot_create_time < cutoff_date and snapshot['Status'] == 'available':
                        try:
                            docdb_client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot_id)
                            results['snapshots_deleted'].append({
                                'ClusterIdentifier': cluster_id_of_snapshot,
                                'SnapshotIdentifier': snapshot_id
                            })
                            if sns:
                                sns.publish(
                                    TopicArn=sns_topic_arn,
                                    Message=f"Successfully deleted old DocumentDB snapshot {snapshot_id} for cluster {cluster_id_of_snapshot}.",
                                    Subject=f"DocumentDB Old Snapshot Deleted: {cluster_id_of_snapshot}"
                                )
                        except Exception as e_del:
                            error_msg = f"Error deleting DocumentDB snapshot {snapshot_id}: {str(e_del)}"
                            results['errors'].append(error_msg)
                            if sns:
                                sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"DocumentDB Snapshot Deletion Error")
        except Exception as e_desc:
            error_msg = f"Error describing DocumentDB snapshots for deletion: {str(e_desc)}"
            results['errors'].append(error_msg)
            if sns:
                sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject="DocumentDB Snapshot Management Error")

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['DOCDB_CLUSTER_IDENTIFIERS'] = 'your-docdb-cluster-id' # Replace
    os.environ['RETENTION_DAYS'] = '7'
    os.environ['ACTION'] = 'all' # or 'backup' or 'cleanup'
    # os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic'
    print(lambda_handler({}, {}))