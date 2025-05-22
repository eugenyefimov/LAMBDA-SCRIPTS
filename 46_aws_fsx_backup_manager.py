import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage AWS FSx backups.
    
    This function can:
    - Create backups for specified FSx file systems.
    - Delete old backups based on a retention policy.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - FILESYSTEM_IDS: Comma-separated list of FSx File System IDs to back up.
    - RETENTION_DAYS: Number of days to retain backups (e.g., 7, 30).
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications.
    - BACKUP_TAGS: Optional. JSON string of tags to apply to backups. E.g., '[{"Key":"Environment","Value":"Production"}]'
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    filesystem_ids_str = os.environ.get('FILESYSTEM_IDS')
    retention_days_str = os.environ.get('RETENTION_DAYS')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    backup_tags_str = os.environ.get('BACKUP_TAGS', '[]')

    if not filesystem_ids_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required environment variable: FILESYSTEM_IDS')}
    if not retention_days_str:
        return {'statusCode': 400, 'body': json.dumps('Missing required environment variable: RETENTION_DAYS')}

    try:
        retention_days = int(retention_days_str)
    except ValueError:
        return {'statusCode': 400, 'body': json.dumps('Invalid RETENTION_DAYS value. Must be an integer.')}

    try:
        backup_tags = json.loads(backup_tags_str)
        if not isinstance(backup_tags, list):
            raise ValueError("BACKUP_TAGS must be a JSON list of Key-Value pairs.")
    except json.JSONDecodeError as e:
        return {'statusCode': 400, 'body': json.dumps(f'Invalid JSON in BACKUP_TAGS: {str(e)}')}
    except ValueError as e:
        return {'statusCode': 400, 'body': json.dumps(str(e))}


    filesystem_ids = [fsid.strip() for fsid in filesystem_ids_str.split(',')]

    # Initialize AWS clients
    fsx_client = boto3.client('fsx', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'backups_created': [],
        'backups_deleted': [],
        'errors': []
    }

    # Create backups
    for fs_id in filesystem_ids:
        try:
            backup_response = fsx_client.create_backup(
                FileSystemId=fs_id,
                Tags=backup_tags
            )
            backup_id = backup_response['Backup']['BackupId']
            results['backups_created'].append({'FileSystemId': fs_id, 'BackupId': backup_id})
            if sns:
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Message=f"Successfully created FSx backup {backup_id} for file system {fs_id}.",
                    Subject=f"FSx Backup Created: {fs_id}"
                )
        except Exception as e:
            error_msg = f"Error creating backup for FSx {fs_id}: {str(e)}"
            results['errors'].append(error_msg)
            if sns:
                sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"FSx Backup Creation Error: {fs_id}")

    # Delete old backups
    cutoff_date = datetime.now(datetime.utcnow().tzinfo) - timedelta(days=retention_days)
    
    try:
        paginator = fsx_client.get_paginator('describe_backups')
        for page in paginator.paginate():
            for backup in page.get('Backups', []):
                backup_id = backup['BackupId']
                fs_id_of_backup = backup.get('FileSystem', {}).get('FileSystemId', 'UnknownFS')
                
                # Only manage backups for the specified file systems if FILESYSTEM_IDS is provided.
                # If we want this lambda to only manage backups it creates, we'd need to tag backups
                # and filter by that tag. For now, it deletes old backups for the specified FSx systems.
                if fs_id_of_backup not in filesystem_ids:
                    continue

                backup_creation_time = backup['CreationTime']
                if backup_creation_time < cutoff_date and backup['Lifecycle'] == 'AVAILABLE':
                    try:
                        fsx_client.delete_backup(BackupId=backup_id)
                        results['backups_deleted'].append({'FileSystemId': fs_id_of_backup, 'BackupId': backup_id})
                        if sns:
                            sns.publish(
                                TopicArn=sns_topic_arn,
                                Message=f"Successfully deleted old FSx backup {backup_id} for file system {fs_id_of_backup}.",
                                Subject=f"FSx Old Backup Deleted: {fs_id_of_backup}"
                            )
                    except Exception as e_del:
                        error_msg = f"Error deleting FSx backup {backup_id} for {fs_id_of_backup}: {str(e_del)}"
                        results['errors'].append(error_msg)
                        if sns:
                            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"FSx Backup Deletion Error: {fs_id_of_backup}")
    except Exception as e_desc:
        error_msg = f"Error describing FSx backups for deletion: {str(e_desc)}"
        results['errors'].append(error_msg)
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject="FSx Backup Management Error")


    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['FILESYSTEM_IDS'] = 'fs-xxxxxxxxxxxxxxxxx' # Replace with your FSx File System ID
    os.environ['RETENTION_DAYS'] = '7'
    # os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic'
    # os.environ['BACKUP_TAGS'] = '[{"Key":"BackupType","Value":"AutomatedLambda"}]'
    print(lambda_handler({}, {}))