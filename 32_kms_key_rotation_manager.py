import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage KMS key rotation.
    
    This function automates KMS key management:
    - Ensures key rotation is enabled for eligible keys
    - Identifies keys nearing expiration
    - Tracks key usage and recommends cleanup of unused keys
    - Generates key inventory reports
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ENFORCE_ROTATION: Whether to enforce key rotation (default: true)
    - UNUSED_KEY_THRESHOLD_DAYS: Days of inactivity before flagging a key as unused (default: 90)
    - EXPIRY_WARNING_DAYS: Days before expiration to start warning (default: 30)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    - S3_REPORT_BUCKET: Optional S3 bucket for storing reports
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    enforce_rotation = os.environ.get('ENFORCE_ROTATION', 'true').lower() == 'true'
    unused_threshold_days = int(os.environ.get('UNUSED_KEY_THRESHOLD_DAYS', 90))
    expiry_warning_days = int(os.environ.get('EXPIRY_WARNING_DAYS', 30))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    s3_report_bucket = os.environ.get('S3_REPORT_BUCKET', '')
    
    # Initialize AWS clients
    kms = boto3.client('kms', region_name=region)
    cloudtrail = boto3.client('cloudtrail', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    s3 = boto3.client('s3', region_name=region) if s3_report_bucket else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'rotation_enabled': [],
        'rotation_failed': [],
        'expiring_keys': [],
        'unused_keys': [],
        'key_inventory': [],
        'errors': []
    }
    
    # Get all customer managed KMS keys
    try:
        paginator = kms.get_paginator('list_keys')
        all_keys = []
        for page in paginator.paginate():
            all_keys.extend(page['Keys'])
        
        # Calculate cutoff date for unused keys
        unused_cutoff_date = datetime.now() - timedelta(days=unused_threshold_days)
        expiry_cutoff_date = datetime.now() + timedelta(days=expiry_warning_days)
        
        for key in all_keys:
            key_id = key['KeyId']
            
            try:
                # Get key details
                key_info = kms.describe_key(KeyId=key_id)
                
                # Skip AWS managed keys
                if key_info['KeyMetadata']['KeyManager'] == 'AWS':
                    continue
                
                # Add to key inventory
                key_metadata = key_info['KeyMetadata']
                key_inventory_item = {
                    'KeyId': key_id,
                    'Arn': key_metadata['Arn'],
                    'Description': key_metadata.get('Description', ''),
                    'Enabled': key_metadata['KeyState'] == 'Enabled',
                    'KeyState': key_metadata['KeyState'],
                    'CreationDate': key_metadata['CreationDate'].isoformat() if 'CreationDate' in key_metadata else None,
                    'KeyUsage': key_metadata.get('KeyUsage', ''),
                    'Origin': key_metadata.get('Origin', ''),
                    'ExpirationModel': key_metadata.get('ExpirationModel', '')
                }
                
                # Get key rotation status
                try:
                    rotation_status = kms.get_key_rotation_status(KeyId=key_id)
                    key_inventory_item['RotationEnabled'] = rotation_status.get('KeyRotationEnabled', False)
                    
                    # Enable rotation if required and not already enabled
                    if enforce_rotation and not rotation_status.get('KeyRotationEnabled', False):
                        # Only customer-managed CMKs can have rotation enabled
                        if key_metadata['KeyManager'] == 'CUSTOMER' and key_metadata['KeyState'] == 'Enabled':
                            try:
                                kms.enable_key_rotation(KeyId=key_id)
                                results['rotation_enabled'].append(key_id)
                                key_inventory_item['RotationEnabled'] = True
                            except Exception as e:
                                error_msg = f"Failed to enable rotation for key {key_id}: {str(e)}"
                                results['rotation_failed'].append({'KeyId': key_id, 'Error': str(e)})
                                results['errors'].append(error_msg)
                                print(error_msg)
                except Exception as e:
                    key_inventory_item['RotationEnabled'] = 'Unknown'
                    error_msg = f"Error getting rotation status for key {key_id}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(error_msg)
                
                # Get key aliases
                try:
                    aliases_response = kms.list_aliases(KeyId=key_id)
                    key_inventory_item['Aliases'] = [alias['AliasName'] for alias in aliases_response.get('Aliases', [])]
                except Exception:
                    key_inventory_item['Aliases'] = []
                
                # Get key tags
                try:
                    tags_response = kms.list_resource_tags(KeyId=key_id)
                    key_inventory_item['Tags'] = tags_response.get('Tags', [])
                except Exception:
                    key_inventory_item['Tags'] = []
                
                # Check for key expiration
                if 'ExpirationDate' in key_metadata:
                    expiration_date = key_metadata['ExpirationDate']
                    key_inventory_item['ExpirationDate'] = expiration_date.isoformat()
                    
                    if expiration_date <= expiry_cutoff_date:
                        results['expiring_keys'].append({
                            'KeyId': key_id,
                            'ExpirationDate': expiration_date.isoformat(),
                            'DaysUntilExpiration': (expiration_date - datetime.now()).days
                        })
                
                # Check for key usage
                try:
                    # Look for recent usage in CloudTrail
                    usage_found = False
                    
                    # Check CloudTrail for key usage events
                    cloudtrail_response = cloudtrail.lookup_events(
                        LookupAttributes=[
                            {
                                'AttributeKey': 'ResourceName',
                                'AttributeValue': key_id
                            }
                        ],
                        StartTime=unused_cutoff_date,
                        EndTime=datetime.now()
                    )
                    
                    if cloudtrail_response['Events']:
                        usage_found = True
                    
                    if not usage_found and key_metadata['KeyState'] == 'Enabled':
                        results['unused_keys'].append({
                            'KeyId': key_id,
                            'DaysUnused': unused_threshold_days,
                            'Aliases': key_inventory_item['Aliases']
                        })
                except Exception as e:
                    error_msg = f"Error checking usage for key {key_id}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(error_msg)
                
                # Add to inventory
                results['key_inventory'].append(key_inventory_item)
                
            except Exception as e:
                error_msg = f"Error processing key {key_id}: {str(e)}"
                results['errors'].append(error_msg)
                print(error_msg)
    
    except Exception as e:
        error_msg = f"Error listing KMS keys: {str(e)}"
        results['errors'].append(error_msg)
        print(error_msg)
    
    # Generate report in S3 if configured
    if s3 and s3_report_bucket:
        try:
            report_key = f"kms-reports/key-inventory-{datetime.now().strftime('%Y-%m-%d')}.json"
            s3.put_object(
                Bucket=s3_report_bucket,
                Key=report_key,
                Body=json.dumps(results, default=str, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            error_msg = f"Error saving report to S3: {str(e)}"
            results['errors'].append(error_msg)
            print(error_msg)
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn and (results['rotation_enabled'] or results['rotation_failed'] or 
                                 results['expiring_keys'] or results['unused_keys'] or 
                                 results['errors']):
        try:
            # Create a summary for the SNS message
            summary = {
                'timestamp': results['timestamp'],
                'rotation_enabled_count': len(results['rotation_enabled']),
                'rotation_failed_count': len(results['rotation_failed']),
                'expiring_keys_count': len(results['expiring_keys']),
                'unused_keys_count': len(results['unused_keys']),
                'total_keys_count': len(results['key_inventory']),
                'errors_count': len(results['errors'])
            }
            
            # Include details for important findings
            if results['rotation_failed']:
                summary['rotation_failed'] = results['rotation_failed']
            if results['expiring_keys']:
                summary['expiring_keys'] = results['expiring_keys']
            if results['unused_keys']:
                summary['unused_keys'] = results['unused_keys']
            if results['errors']:
                summary['errors'] = results['errors']
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"KMS Key Rotation Manager Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps(summary, default=str, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({k: v for k, v in results.items() if k != 'key_inventory'}, default=str)
    }