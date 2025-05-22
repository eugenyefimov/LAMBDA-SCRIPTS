import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage DynamoDB table backups.
    
    This function creates on-demand backups for DynamoDB tables and
    deletes old backups based on retention policy.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - TABLE_NAMES: Comma-separated list of table names to backup (optional)
    - BACKUP_RETENTION_DAYS: Number of days to retain backups (default: 30)
    - BACKUP_PREFIX: Prefix for backup names (default: 'automated')
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    table_names_str = os.environ.get('TABLE_NAMES', '')
    retention_days = int(os.environ.get('BACKUP_RETENTION_DAYS', '30'))
    backup_prefix = os.environ.get('BACKUP_PREFIX', 'automated')
    
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    # Get list of tables to backup
    tables_to_backup = []
    if table_names_str:
        tables_to_backup = table_names_str.split(',')
    else:
        paginator = dynamodb.get_paginator('list_tables')
        for page in paginator.paginate():
            tables_to_backup.extend(page['TableNames'])
    
    # Calculate cutoff date for backup deletion
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    created_backups = []
    deleted_backups = []
    errors = []
    
    # Create new backups
    for table_name in tables_to_backup:
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
            backup_name = f"{backup_prefix}-{table_name}-{timestamp}"
            
            response = dynamodb.create_backup(
                TableName=table_name,
                BackupName=backup_name
            )
            
            created_backups.append({
                'table_name': table_name,
                'backup_name': backup_name,
                'backup_arn': response['BackupDetails']['BackupArn']
            })
            
        except Exception as e:
            errors.append({
                'table_name': table_name,
                'operation': 'create_backup',
                'error': str(e)
            })
    
    # Delete old backups
    try:
        paginator = dynamodb.get_paginator('list_backups')
        
        for page in paginator.paginate():
            for backup in page['BackupSummaries']:
                # Only delete backups created by this automation
                if backup['BackupName'].startswith(backup_prefix):
                    backup_creation = backup['BackupCreationDateTime']
                    
                    # Convert to datetime object if it's not already
                    if not isinstance(backup_creation, datetime):
                        backup_creation = datetime.fromtimestamp(backup_creation)
                    
                    if backup_creation < cutoff_date:
                        try:
                            dynamodb.delete_backup(
                                BackupArn=backup['BackupArn']
                            )
                            deleted_backups.append({
                                'backup_name': backup['BackupName'],
                                'backup_arn': backup['BackupArn'],
                                'creation_date': backup_creation.isoformat()
                            })
                        except Exception as e:
                            errors.append({
                                'backup_name': backup['BackupName'],
                                'operation': 'delete_backup',
                                'error': str(e)
                            })
    except Exception as e:
        errors.append({
            'operation': 'list_backups',
            'error': str(e)
        })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'DynamoDB backup management completed',
            'created_backups': created_backups,
            'deleted_backups': deleted_backups,
            'errors': errors,
            'retention_policy': f"{retention_days} days"
        })
    }