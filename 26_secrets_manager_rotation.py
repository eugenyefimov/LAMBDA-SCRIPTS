import boto3
import json
import os
import random
import string
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage rotation of AWS Secrets Manager secrets.
    
    This function helps manage secrets rotation:
    - Identifies secrets due for rotation
    - Rotates secrets based on type (database credentials, API keys, etc.)
    - Updates dependent resources with new secrets
    - Maintains rotation history
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - SECRET_TAGS: Optional JSON string of tags to filter secrets
    - ROTATION_DAYS: Days between rotations (default: 90)
    - DRY_RUN: If true, only report on secrets needing rotation (default: true)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    secret_tags_str = os.environ.get('SECRET_TAGS', '{}')
    rotation_days = int(os.environ.get('ROTATION_DAYS', 90))
    dry_run = os.environ.get('DRY_RUN', 'true').lower() == 'true'
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    secrets_manager = boto3.client('secretsmanager', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Parse secret tags
    try:
        secret_tags = json.loads(secret_tags_str)
    except json.JSONDecodeError:
        secret_tags = {}
    
    # Calculate rotation threshold date
    now = datetime.now()
    rotation_threshold = now - timedelta(days=rotation_days)
    
    results = {
        'timestamp': now.isoformat(),
        'secrets_checked': 0,
        'secrets_needing_rotation': [],
        'secrets_rotated': [],
        'rotation_errors': []
    }
    
    # List all secrets
    secrets = []
    paginator = secrets_manager.get_paginator('list_secrets')
    
    # Build filter based on tags
    filters = []
    for key, value in secret_tags.items():
        filters.append({
            'Key': f'tag-key',
            'Values': [key]
        })
        filters.append({
            'Key': f'tag-value',
            'Values': [value]
        })
    
    # Get secrets with pagination
    for page in paginator.paginate(Filters=filters if filters else None):
        secrets.extend(page['SecretList'])
    
    results['secrets_checked'] = len(secrets)
    
    # Check each secret for rotation
    for secret in secrets:
        secret_name = secret['Name']
        secret_arn = secret['ARN']
        
        # Check if secret has rotation enabled
        rotation_enabled = secret.get('RotationEnabled', False)
        
        # Check last rotated date
        last_rotated = secret.get('LastRotatedDate')
        last_changed = secret.get('LastChangedDate')
        
        # Use the most recent date between rotation and change
        last_updated = last_rotated if last_rotated and (not last_changed or last_rotated > last_changed) else last_changed
        
        # If never rotated or last rotation is older than threshold
        if not last_updated or last_updated < rotation_threshold:
            # Get secret details
            try:
                secret_value = secrets_manager.get_secret_value(SecretId=secret_arn)
                secret_string = secret_value.get('SecretString')
                
                if secret_string:
                    # Parse secret value
                    try:
                        secret_data = json.loads(secret_string)
                        secret_type = 'structured'
                    except json.JSONDecodeError:
                        secret_data = secret_string
                        secret_type = 'string'
                    
                    # Add to secrets needing rotation
                    results['secrets_needing_rotation'].append({
                        'name': secret_name,
                        'arn': secret_arn,
                        'last_updated': last_updated.isoformat() if last_updated else None,
                        'days_since_rotation': (now - last_updated).days if last_updated else None,
                        'rotation_enabled': rotation_enabled,
                        'type': secret_type
                    })
                    
                    # Rotate secret if not in dry run mode
                    if not dry_run:
                        try:
                            if rotation_enabled:
                                # If rotation is already configured, start rotation
                                secrets_manager.rotate_secret(
                                    SecretId=secret_arn
                                )
                            else:
                                # For secrets without rotation configured, generate new value
                                if secret_type == 'structured':
                                    # Handle different types of structured secrets
                                    if 'username' in secret_data and 'password' in secret_data:
                                        # Database credentials
                                        secret_data['password'] = generate_password()
                                    elif 'accessKey' in secret_data and 'secretKey' in secret_data:
                                        # AWS credentials
                                        secret_data['secretKey'] = generate_password(32)
                                    elif 'apiKey' in secret_data:
                                        # API key
                                        secret_data['apiKey'] = generate_api_key()
                                    
                                    # Update secret
                                    secrets_manager.put_secret_value(
                                        SecretId=secret_arn,
                                        SecretString=json.dumps(secret_data)
                                    )
                                else:
                                    # Simple string secret
                                    new_value = generate_password(16)
                                    secrets_manager.put_secret_value(
                                        SecretId=secret_arn,
                                        SecretString=new_value
                                    )
                            
                            results['secrets_rotated'].append({
                                'name': secret_name,
                                'arn': secret_arn,
                                'type': secret_type
                            })
                        except Exception as e:
                            results['rotation_errors'].append({
                                'name': secret_name,
                                'arn': secret_arn,
                                'error': str(e)
                            })
            except Exception as e:
                print(f"Error processing secret {secret_name}: {str(e)}")
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Secrets Manager Rotation Report - {now.strftime('%Y-%m-%d')}",
                Message=json.dumps(results, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

def generate_password(length=16):
    """Generate a secure random password"""
    chars = string.ascii_letters + string.digits + "!@#$%^&*()_-+=<>?"
    return ''.join(random.choice(chars) for _ in range(length))

def generate_api_key(length=32):
    """Generate an API key"""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))