import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to verify AWS Backup jobs and recovery points.
    
    This function checks AWS Backup jobs and recovery points to ensure:
    - Backup jobs completed successfully
    - Recovery points are available
    - Recovery points can be restored
    - Optional test restores can be performed
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - BACKUP_VAULT_NAMES: Comma-separated list of backup vault names to check
    - DAYS_TO_CHECK: Number of days of backup history to verify (default: 7)
    - PERFORM_TEST_RESTORE: Whether to perform test restores (default: false)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    vault_names_str = os.environ.get('BACKUP_VAULT_NAMES', '')
    days_to_check = int(os.environ.get('DAYS_TO_CHECK', 7))
    perform_test_restore = os.environ.get('PERFORM_TEST_RESTORE', 'false').lower() == 'true'
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    backup = boto3.client('backup', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_to_check)
    
    # Parse vault names
    vault_names = [name.strip() for name in vault_names_str.split(',')] if vault_names_str else []
    
    # If no vault names provided, get all vaults
    if not vault_names:
        try:
            response = backup.list_backup_vaults()
            vault_names = [vault['BackupVaultName'] for vault in response['BackupVaultList']]
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing backup vaults',
                    'error': str(e)
                })
            }
    
    results = {
        'verified_vaults': [],
        'failed_jobs': [],
        'missing_recovery_points': [],
        'test_restores': []
    }
    
    # Check each vault
    for vault_name in vault_names:
        vault_result = {
            'vault_name': vault_name,
            'jobs_checked': 0,
            'successful_jobs': 0,
            'recovery_points_checked': 0,
            'valid_recovery_points': 0
        }
        
        # Check backup jobs
        try:
            paginator = backup.get_paginator('list_backup_jobs')
            for page in paginator.paginate(
                ByBackupVaultName=vault_name,
                ByCreatedAfter=start_time,
                ByState='COMPLETED'
            ):
                for job in page['BackupJobs']:
                    vault_result['jobs_checked'] += 1
                    
                    if job['State'] == 'COMPLETED':
                        vault_result['successful_jobs'] += 1
                    else:
                        results['failed_jobs'].append({
                            'job_id': job['BackupJobId'],
                            'resource_type': job['ResourceType'],
                            'resource_arn': job['ResourceArn'],
                            'state': job['State'],
                            'creation_date': job['CreationDate'].isoformat(),
                            'completion_date': job.get('CompletionDate', '').isoformat() if job.get('CompletionDate') else None,
                            'message': job.get('StatusMessage', '')
                        })
        except Exception as e:
            vault_result['error_checking_jobs'] = str(e)
        
        # Check recovery points
        try:
            paginator = backup.get_paginator('list_recovery_points_by_backup_vault')
            for page in paginator.paginate(
                BackupVaultName=vault_name,
                ByCreatedAfter=start_time
            ):
                for recovery_point in page['RecoveryPoints']:
                    vault_result['recovery_points_checked'] += 1
                    
                    # Check if recovery point is valid
                    if recovery_point['Status'] == 'COMPLETED':
                        vault_result['valid_recovery_points'] += 1
                        
                        # Perform test restore if enabled
                        if perform_test_restore:
                            try:
                                # Get recovery point details
                                recovery_point_arn = recovery_point['RecoveryPointArn']
                                resource_type = recovery_point['ResourceType']
                                
                                # Different restore approach based on resource type
                                if resource_type == 'EBS':
                                    # For EBS volumes, we can create a test volume
                                    ec2 = boto3.client('ec2', region_name=region)
                                    response = backup.get_recovery_point_restore_metadata(
                                        BackupVaultName=vault_name,
                                        RecoveryPointArn=recovery_point_arn
                                    )
                                    
                                    # Start a restore job
                                    restore_job = backup.start_restore_job(
                                        RecoveryPointArn=recovery_point_arn,
                                        Metadata={
                                            'AvailabilityZone': ec2.describe_availability_zones()['AvailabilityZones'][0]['ZoneName'],
                                            'RestoreTestOnly': 'true'
                                        },
                                        IamRoleArn=response['RestoreMetadata'].get('IAMRoleARN', '')
                                    )
                                    
                                    results['test_restores'].append({
                                        'recovery_point_arn': recovery_point_arn,
                                        'resource_type': resource_type,
                                        'restore_job_id': restore_job['RestoreJobId'],
                                        'status': 'STARTED'
                                    })
                                    
                            except Exception as e:
                                results['test_restores'].append({
                                    'recovery_point_arn': recovery_point_arn,
                                    'resource_type': resource_type,
                                    'error': str(e)
                                })
                    else:
                        results['missing_recovery_points'].append({
                            'recovery_point_arn': recovery_point['RecoveryPointArn'],
                            'resource_type': recovery_point['ResourceType'],
                            'status': recovery_point['Status'],
                            'creation_date': recovery_point['CreationDate'].isoformat()
                        })
        except Exception as e:
            vault_result['error_checking_recovery_points'] = str(e)
        
        results['verified_vaults'].append(vault_result)
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn:
        try:
            # Prepare summary
            summary = {
                'timestamp': datetime.now().isoformat(),
                'vaults_checked': len(results['verified_vaults']),
                'failed_jobs': len(results['failed_jobs']),
                'missing_recovery_points': len(results['missing_recovery_points']),
                'test_restores': len(results['test_restores'])
            }
            
            # Send notification
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"AWS Backup Verification Report - {end_time.strftime('%Y-%m-%d')}",
                Message=json.dumps({
                    'summary': summary,
                    'details': results
                }, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Backup verification completed',
            'time_range': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            },
            'results': results
        })
    }