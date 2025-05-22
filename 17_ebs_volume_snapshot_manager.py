import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage EBS volume snapshots.
    
    This function automates EBS volume snapshot management:
    - Creates snapshots of volumes with specific tags
    - Deletes snapshots older than a specified retention period
    - Tags snapshots with metadata for easier management
    - Optionally copies snapshots to another region for disaster recovery
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - TARGET_REGION: Region to copy snapshots to (optional)
    - RETENTION_DAYS: Number of days to retain snapshots (default: 7)
    - TAG_KEY: Tag key to identify volumes to snapshot (default: Backup)
    - TAG_VALUE: Tag value to identify volumes to snapshot (default: true)
    - COPY_SNAPSHOTS: Whether to copy snapshots to target region (default: false)
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    target_region = os.environ.get('TARGET_REGION', '')
    retention_days = int(os.environ.get('RETENTION_DAYS', 7))
    tag_key = os.environ.get('TAG_KEY', 'Backup')
    tag_value = os.environ.get('TAG_VALUE', 'true')
    copy_snapshots = os.environ.get('COPY_SNAPSHOTS', 'false').lower() == 'true'
    
    # Initialize EC2 client
    ec2 = boto3.client('ec2', region_name=region)
    target_ec2 = boto3.client('ec2', region_name=target_region) if target_region and copy_snapshots else None
    
    # Get current timestamp for tagging
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    
    # Calculate retention date
    retention_date = datetime.now() - timedelta(days=retention_days)
    
    # Find volumes to snapshot
    volumes_response = ec2.describe_volumes(
        Filters=[
            {
                'Name': f'tag:{tag_key}',
                'Values': [tag_value]
            }
        ]
    )
    
    snapshots_created = []
    snapshots_copied = []
    snapshots_deleted = []
    errors = []
    
    # Create snapshots
    for volume in volumes_response['Volumes']:
        volume_id = volume['VolumeId']
        
        # Get volume name tag if it exists
        volume_name = 'unnamed'
        for tag in volume.get('Tags', []):
            if tag['Key'] == 'Name':
                volume_name = tag['Value']
                break
        
        try:
            # Create snapshot
            snapshot_response = ec2.create_snapshot(
                VolumeId=volume_id,
                Description=f"Automated snapshot of {volume_id} ({volume_name}) - {timestamp}"
            )
            
            snapshot_id = snapshot_response['SnapshotId']
            
            # Tag the snapshot
            ec2.create_tags(
                Resources=[snapshot_id],
                Tags=[
                    {'Key': 'Name', 'Value': f"Snapshot-{volume_name}-{timestamp}"},
                    {'Key': 'CreatedBy', 'Value': 'Lambda-EBS-Snapshot-Manager'},
                    {'Key': 'SourceVolumeId', 'Value': volume_id},
                    {'Key': 'CreationDate', 'Value': timestamp},
                    {'Key': 'RetentionDays', 'Value': str(retention_days)}
                ]
            )
            
            snapshots_created.append({
                'snapshot_id': snapshot_id,
                'volume_id': volume_id,
                'volume_name': volume_name
            })
            
            # Copy snapshot to target region if enabled
            if target_region and copy_snapshots:
                try:
                    copy_response = target_ec2.copy_snapshot(
                        SourceRegion=region,
                        SourceSnapshotId=snapshot_id,
                        Description=f"Copy of {snapshot_id} from {region} - {timestamp}"
                    )
                    
                    target_snapshot_id = copy_response['SnapshotId']
                    
                    # Tag the copied snapshot
                    target_ec2.create_tags(
                        Resources=[target_snapshot_id],
                        Tags=[
                            {'Key': 'Name', 'Value': f"Snapshot-{volume_name}-{timestamp}"},
                            {'Key': 'CreatedBy', 'Value': 'Lambda-EBS-Snapshot-Manager'},
                            {'Key': 'SourceVolumeId', 'Value': volume_id},
                            {'Key': 'SourceRegion', 'Value': region},
                            {'Key': 'SourceSnapshotId', 'Value': snapshot_id},
                            {'Key': 'CreationDate', 'Value': timestamp},
                            {'Key': 'RetentionDays', 'Value': str(retention_days)}
                        ]
                    )
                    
                    snapshots_copied.append({
                        'source_snapshot_id': snapshot_id,
                        'target_snapshot_id': target_snapshot_id,
                        'volume_id': volume_id,
                        'target_region': target_region
                    })
                except Exception as e:
                    errors.append({
                        'operation': 'copy_snapshot',
                        'snapshot_id': snapshot_id,
                        'target_region': target_region,
                        'error': str(e)
                    })
        except Exception as e:
            errors.append({
                'operation': 'create_snapshot',
                'volume_id': volume_id,
                'error': str(e)
            })
    
    # Delete old snapshots
    try:
        snapshots_response = ec2.describe_snapshots(
            Filters=[
                {
                    'Name': 'tag:CreatedBy',
                    'Values': ['Lambda-EBS-Snapshot-Manager']
                }
            ],
            OwnerIds=['self']
        )
        
        for snapshot in snapshots_response['Snapshots']:
            snapshot_id = snapshot['SnapshotId']
            start_time = snapshot['StartTime']
            
            # Convert to datetime object
            if isinstance(start_time, datetime):
                snapshot_date = start_time
            else:
                # If it's a string, parse it
                snapshot_date = datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S.%f%z')
            
            # Check if snapshot is older than retention period
            if snapshot_date.replace(tzinfo=None) < retention_date:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                    snapshots_deleted.append(snapshot_id)
                except Exception as e:
                    errors.append({
                        'operation': 'delete_snapshot',
                        'snapshot_id': snapshot_id,
                        'error': str(e)
                    })
    except Exception as e:
        errors.append({
            'operation': 'list_snapshots',
            'error': str(e)
        })
    
    # Delete old snapshots in target region if enabled
    if target_region and copy_snapshots:
        try:
            target_snapshots_response = target_ec2.describe_snapshots(
                Filters=[
                    {
                        'Name': 'tag:CreatedBy',
                        'Values': ['Lambda-EBS-Snapshot-Manager']
                    },
                    {
                        'Name': 'tag:SourceRegion',
                        'Values': [region]
                    }
                ],
                OwnerIds=['self']
            )
            
            for snapshot in target_snapshots_response['Snapshots']:
                snapshot_id = snapshot['SnapshotId']
                start_time = snapshot['StartTime']
                
                # Convert to datetime object
                if isinstance(start_time, datetime):
                    snapshot_date = start_time
                else:
                    # If it's a string, parse it
                    snapshot_date = datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S.%f%z')
                
                # Check if snapshot is older than retention period
                if snapshot_date.replace(tzinfo=None) < retention_date:
                    try:
                        target_ec2.delete_snapshot(SnapshotId=snapshot_id)
                        snapshots_deleted.append({
                            'snapshot_id': snapshot_id,
                            'region': target_region
                        })
                    except Exception as e:
                        errors.append({
                            'operation': 'delete_snapshot_target_region',
                            'snapshot_id': snapshot_id,
                            'region': target_region,
                            'error': str(e)
                        })
        except Exception as e:
            errors.append({
                'operation': 'list_snapshots_target_region',
                'region': target_region,
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'EBS snapshot management completed',
            'timestamp': datetime.now().isoformat(),
            'snapshots_created': snapshots_created,
            'snapshots_copied': snapshots_copied,
            'snapshots_deleted': snapshots_deleted,
            'errors': errors
        })
    }