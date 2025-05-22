import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage Amazon Neptune database backups.
    
    This function automates Neptune database backup management:
    - Creates manual snapshots of Neptune clusters
    - Manages snapshot retention based on configurable policies
    - Copies snapshots to another region for disaster recovery
    - Monitors backup status and sends notifications
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - CLUSTER_IDENTIFIERS: Comma-separated list of Neptune cluster IDs
    - RETENTION_DAYS: Number of days to retain snapshots (default: 7)
    - CROSS_REGION_COPY: Whether to copy snapshots to another region (default: false)
    - DESTINATION_REGION: Region to copy snapshots to (required if CROSS_REGION_COPY is true)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    cluster_ids_str = os.environ.get('CLUSTER_IDENTIFIERS', '')
    retention_days = int(os.environ.get('RETENTION_DAYS', 7))
    cross_region_copy = os.environ.get('CROSS_REGION_COPY', 'false').lower() == 'true'
    destination_region = os.environ.get('DESTINATION_REGION', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    neptune = boto3.client('neptune', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    dest_neptune = boto3.client('neptune', region_name=destination_region) if cross_region_copy and destination_region else None
    
    # Parse cluster IDs
    cluster_ids = [id.strip() for id in cluster_ids_str.split(',')] if cluster_ids_str else []
    
    # If no cluster IDs provided, get all Neptune clusters
    if not cluster_ids:
        try:
            response = neptune.describe_db_clusters()
            cluster_ids = [cluster['DBClusterIdentifier'] for cluster in response['DBClusters'] 
                          if cluster['Engine'] == 'neptune']
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing Neptune clusters',
                    'error': str(e)
                })
            }
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'snapshots_created': [],
        'snapshots_copied': [],
        'snapshots_deleted': [],
        'errors': []
    }
    
    # Create snapshots for each cluster
    for cluster_id in cluster_ids:
        try:
            # Create snapshot name with timestamp
            snapshot_id = f"{cluster_id}-{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
            
            # Create snapshot
            neptune.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snapshot_id,
                DBClusterIdentifier=cluster_id
            )
            
            results['snapshots_created'].append({
                'cluster_id': cluster_id,
                'snapshot_id': snapshot_id
            })
            
            # Copy snapshot to destination region if enabled
            if cross_region_copy and dest_neptune:
                try:
                    dest_snapshot_id = f"copy-{snapshot_id}"
                    dest_neptune.copy_db_cluster_snapshot(
                        SourceDBClusterSnapshotIdentifier=f"arn:aws:rds:{region}:{boto3.client('sts').get_caller_identity()['Account']}:cluster-snapshot:{snapshot_id}",
                        TargetDBClusterSnapshotIdentifier=dest_snapshot_id,
                        SourceRegion=region
                    )
                    
                    results['snapshots_copied'].append({
                        'source_snapshot_id': snapshot_id,
                        'destination_snapshot_id': dest_snapshot_id,
                        'destination_region': destination_region
                    })
                except Exception as e:
                    error_msg = f"Error copying snapshot {snapshot_id} to {destination_region}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(error_msg)
            
        except Exception as e:
            error_msg = f"Error creating snapshot for cluster {cluster_id}: {str(e)}"
            results['errors'].append(error_msg)
            print(error_msg)
    
    # Delete old snapshots
    try:
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        # Get all manual snapshots
        response = neptune.describe_db_cluster_snapshots(
            SnapshotType='manual'
        )
        
        for snapshot in response['DBClusterSnapshots']:
            # Check if snapshot is for one of our clusters and is older than retention period
            if (snapshot['Engine'] == 'neptune' and
                snapshot['DBClusterIdentifier'] in cluster_ids and
                snapshot['SnapshotCreateTime'] < cutoff_date):
                
                try:
                    neptune.delete_db_cluster_snapshot(
                        DBClusterSnapshotIdentifier=snapshot['DBClusterSnapshotIdentifier']
                    )
                    
                    results['snapshots_deleted'].append({
                        'snapshot_id': snapshot['DBClusterSnapshotIdentifier'],
                        'creation_date': snapshot['SnapshotCreateTime'].isoformat(),
                        'cluster_id': snapshot['DBClusterIdentifier']
                    })
                except Exception as e:
                    error_msg = f"Error deleting snapshot {snapshot['DBClusterSnapshotIdentifier']}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(error_msg)
    
    except Exception as e:
        error_msg = f"Error listing or deleting old snapshots: {str(e)}"
        results['errors'].append(error_msg)
        print(error_msg)
    
    # Send SNS notification if configured
    if sns and sns_topic_arn:
        try:
            message = {
                'timestamp': results['timestamp'],
                'snapshots_created_count': len(results['snapshots_created']),
                'snapshots_copied_count': len(results['snapshots_copied']),
                'snapshots_deleted_count': len(results['snapshots_deleted']),
                'errors_count': len(results['errors'])
            }
            
            # Include details if there are errors
            if results['errors']:
                message['errors'] = results['errors']
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Neptune Backup Manager Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps(message, indent=2, default=str)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }