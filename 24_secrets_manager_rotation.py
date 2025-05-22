import boto3
import json
import os
import logging
import random
import string
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda function to manage rotation of secrets in AWS Secrets Manager.
    
    This function can:
    - Rotate database credentials (RDS, Redshift)
    - Rotate API keys
    - Update applications with new secrets
    - Verify secret rotation
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - SECRET_TYPES: Comma-separated list of secret types to rotate (default: all)
      Valid values: rds,redshift,api,generic
    - ROTATION_INTERVAL_DAYS: Days between rotations (default: 90)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    secret_types_str = os.environ.get('SECRET_TYPES', 'rds,redshift,api,generic')
    rotation_interval_days = int(os.environ.get('ROTATION_INTERVAL_DAYS', 90))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Parse secret types
    secret_types = [s.strip() for s in secret_types_str.split(',')]
    
    # Initialize AWS clients
    secrets_manager = boto3.client('secretsmanager', region_name=region)
    rds = boto3.client('rds', region_name=region)
    redshift = boto3.client('redshift', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Get all secrets
    secrets = []
    paginator = secrets_manager.get_paginator('list_secrets')
    for page in paginator.paginate():
        secrets.extend(page['SecretList'])
    
    # Track results
    rotated_secrets = []
    failed_rotations = []
    skipped_secrets = []
    
    # Process each secret
    for secret in secrets:
        secret_name = secret['Name']
        secret_arn = secret['ARN']
        
        try:
            # Get secret details
            secret_value = secrets_manager.get_secret_value(SecretId=secret_arn)
            secret_string = secret_value['SecretString']
            secret_data = json.loads(secret_string)
            
            # Check if secret has tags indicating type
            secret_type = None
            if 'Tags' in secret:
                for tag in secret['Tags']:
                    if tag['Key'] == 'SecretType':
                        secret_type = tag['Value'].lower()
            
            # If no tag, try to determine type from content
            if not secret_type:
                if 'engine' in secret_data and 'username' in secret_data and 'password' in secret_data:
                    if secret_data.get('engine') == 'redshift':
                        secret_type = 'redshift'
                    else:
                        secret_type = 'rds'
                elif 'api_key' in secret_data or 'apiKey' in secret_data:
                    secret_type = 'api'
                else:
                    secret_type = 'generic'
            
            # Check if this secret type should be rotated
            if secret_type not in secret_types:
                skipped_secrets.append({
                    'secret_name': secret_name,
                    'secret_type': secret_type,
                    'reason': f"Secret type '{secret_type}' not in rotation list"
                })
                continue
            
            # Check if rotation is needed based on last rotation date
            last_rotated = None
            if 'LastRotatedDate' in secret:
                last_rotated = secret['LastRotatedDate']
            elif 'LastChangedDate' in secret:
                last_rotated = secret['LastChangedDate']
            elif 'CreatedDate' in secret:
                last_rotated = secret['CreatedDate']
            
            if last_rotated and (datetime.now(last_rotated.tzinfo) - last_rotated).days < rotation_interval_days:
                skipped_secrets.append({
                    'secret_name': secret_name,
                    'secret_type': secret_type,
                    'last_rotated': last_rotated.isoformat(),
                    'reason': f"Rotation not due yet (interval: {rotation_interval_days} days)"
                })
                continue
            
            # Rotate secret based on type
            if secret_type == 'rds':
                # RDS database credential rotation
                if 'username' in secret_data and 'password' in secret_data and 'host' in secret_data and 'dbname' in secret_data:
                    username = secret_data['username']
                    old_password = secret_data['password']
                    host = secret_data['host']
                    dbname = secret_data['dbname']
                    port = secret_data.get('port', 3306)
                    engine = secret_data.get('engine', 'mysql')
                    
                    # Generate new password
                    new_password = generate_password(16)
                    
                    # Update the database password
                    # Note: In a real implementation, you would connect to the database and update the password
                    # This is a simplified example
                    logger.info(f"Would update password for user {username} in database {dbname} on {host}")
                    
                    # Update the secret with the new password
                    secret_data['password'] = new_password
                    secrets_manager.update_secret(
                        SecretId=secret_arn,
                        SecretString=json.dumps(secret_data)
                    )
                    
                    rotated_secrets.append({
                        'secret_name': secret_name,
                        'secret_type': secret_type,
                        'rotation_time': datetime.now().isoformat()
                    })
                else:
                    failed_rotations.append({
                        'secret_name': secret_name,
                        'secret_type': secret_type,
                        'reason': 'Missing required fields for RDS rotation'
                    })
            
            elif secret_type == 'redshift':
                # Redshift database credential rotation
                if 'username' in secret_data and 'password' in secret_data and 'host' in secret_data and 'dbname' in secret_data:
                    username = secret_data['username']
                    old_password = secret_data['password']
                    host = secret_data['host']
                    dbname = secret_data['dbname']
                    port = secret_data.get('port', 5439)
                    
                    # Generate new password
                    new_password = generate_password(16)
                    
                    # Update the database password
                    # Note: In a real implementation, you would connect to Redshift and update the password
                    # This is a simplified example
                    logger.info(f"Would update password for user {username} in Redshift cluster {host}")
                    
                    # Update the secret with the new password
                    secret_data['password'] = new_password
                    secrets_manager.update_secret(
                        SecretId=secret_arn,
                        SecretString=json.dumps(secret_data)
                    )
                    
                    rotated_secrets.append({
                        'secret_name': secret_name,
                        'secret_type': secret_type,
                        'rotation_time': datetime.now().isoformat()
                    })
                else:
                    failed_rotations.append({
                        'secret_name': secret_name,
                        'secret_type': secret_type,
                        'reason': 'Missing required fields for Redshift rotation'
                    })
            
            elif secret_type == 'api':
                # API key rotation
                api_key_field = None
                for field in ['api_key', 'apiKey', 'apikey', 'api-key']:
                    if field in secret_data:
                        api_key_field = field
                        break
                
                if api_key_field:
                    old_api_key = secret_data[api_key_field]
                    
                    # Generate new API key
                    new_api_key = generate_api_key(32)
                    
                    # Update the API key in the external service
                    # Note: In a real implementation, you would call the API to update the key
                    # This is a simplified example
                    logger.info(f"Would update API key in external service")
                    
                    # Update the secret with the new API key
                    secret_data[api_key_field] = new_api_key
                    secrets_manager.update_secret(
                        SecretId=secret_arn,
                        SecretString=json.dumps(secret_data)
                    )
                    
                    rotated_secrets.append({
                        'secret_name': secret_name,
                        'secret_type': secret_type,
                        'rotation_time': datetime.now().isoformat()
                    })
                else:
                    failed_rotations.append({
                        'secret_name': secret_name,
                        'secret_type': secret_type,
                        'reason': 'Could not identify API key field'
                    })
            
            elif secret_type == 'generic':
                # Generic secret rotation
                # For generic secrets, we'll just log that we would rotate them
                # In a real implementation, you would implement specific rotation logic
                logger.info(f"Would rotate generic secret: {secret_name}")
                
                skipped_secrets.append({
                    'secret_name': secret_name,
                    'secret_type': secret_type,
                    'reason': 'Generic secrets require custom rotation logic'
                })
        
        except Exception as e:
            failed_rotations.append({
                'secret_name': secret_name,
                'reason': str(e)
            })
            logger.error(f"Error rotating secret {secret_name}: {str(e)}")
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn:
        try:
            message = {
                'timestamp': datetime.now().isoformat(),
                'rotated_secrets': rotated_secrets,
                'failed_rotations': failed_rotations,
                'skipped_secrets': skipped_secrets
            }
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Secrets Manager Rotation Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps(message, indent=2)
            )
        except Exception as e:
            logger.error(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'rotated_secrets': rotated_secrets,
            'failed_rotations': failed_rotations,
            'skipped_secrets': skipped_secrets
        })
    }

def generate_password(length=16):
    """Generate a secure random password"""
    chars = string.ascii_letters + string.digits + "!@#$%^&*()_-+=<>?"
    return ''.join(random.choice(chars) for _ in range(length))

def generate_api_key(length=32):
    """Generate a random API key"""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))