import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage RDS snapshots.
    
    This function creates snapshots of specified RDS instances and deletes
    snapshots older than the retention period.
    
    Environment Variables:
    - DB_INSTANCES: Comma-separated list of RDS instance identifiers
    - RETENTION_DAYS: Number of days to keep snapshots (default: 7)
    - SNAPSHOT_PREFIX: Prefix for snapshot identifiers (default: 'auto')
    - REGION: AWS region (default: us-east-1)
    """
    # Get configuration from environment variables
    db_instances_str = os.environ.get('DB_INSTANCES', '')
    retention_days = int(os.environ.get('RETENTION_DAYS', 7))
    snapshot_prefix = os.environ.get('SNAPSHOT_PREFIX', 'auto')
    region = os.environ.get('REGION', 'us-east-1')
    
    if not db_instances_str:
        return {
            'statusCode': 400,
            'body': json.dumps('No DB instances specified in DB_INSTANCES environment variable')
        }
    
    db_instances = [instance.strip() for instance in db_instances_str.split(',')]
    
    rds = boto3.client('rds', region_name=region)
    created_snapshots = []
    deleted_snapshots = []
    
    # Create new snapshots
    for db_instance in db_instances:
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
        snapshot_id = f"{snapshot_prefix}-{db_instance}-{timestamp}"
        
        try:
            rds.create_db_snapshot(
                DBSnapshotIdentifier=snapshot_id,
                DBInstanceIdentifier=db_instance,
                Tags=[
                    {
                        'Key': 'CreatedBy',
                        'Value': 'LambdaSnapshotManager'
                    },
                    {
                        'Key': 'CreationDate',
                        'Value': datetime.now().strftime('%Y-%m-%d')
                    }
                ]
            )
            created_snapshots.append({
                'DBInstance': db_instance,
                'SnapshotId': snapshot_id
            })
        except Exception as e:
            created_snapshots.append({
                'DBInstance': db_instance,
                'Error': str(e)
            })
    
    # Delete old snapshots
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    # Get all automated snapshots with our prefix
    response = rds.describe_db_snapshots(
        SnapshotType='manual',
        IncludeShared=False,
        IncludePublic=False
    )
    
    for snapshot in response['DBSnapshots']:
        snapshot_id = snapshot['DBSnapshotIdentifier']
        
        # Check if this is one of our managed snapshots
        if snapshot_id.startswith(snapshot_prefix):
            snapshot_create_time = snapshot['SnapshotCreateTime']
            
            # Delete if older than retention period
            if snapshot_create_time.replace(tzinfo=None) < cutoff_date:
                try:
                    rds.delete_db_snapshot(
                        DBSnapshotIdentifier=snapshot_id
                    )
                    deleted_snapshots.append(snapshot_id)
                except Exception as e:
                    deleted_snapshots.append({
                        'SnapshotId': snapshot_id,
                        'Error': str(e)
                    })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'RDS snapshot management completed',
            'created_snapshots': created_snapshots,
            'deleted_snapshots': deleted_snapshots
        })
    }