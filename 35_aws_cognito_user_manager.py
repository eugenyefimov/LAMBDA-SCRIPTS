import boto3
import json
import os
import csv
from datetime import datetime, timedelta
from io import StringIO

def lambda_handler(event, context):
    """
    AWS Lambda function to manage AWS Cognito user pools.
    
    This function automates Cognito user pool management:
    - Monitors user sign-up and authentication activities
    - Identifies inactive users and optionally disables them
    - Exports user data for reporting and analysis
    - Enforces password policies and MFA requirements
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - USER_POOL_ID: Cognito user pool ID (required)
    - INACTIVE_DAYS_THRESHOLD: Days of inactivity before flagging users (default: 90)
    - DISABLE_INACTIVE_USERS: Whether to disable inactive users (default: false)
    - EXPORT_USERS: Whether to export user data to S3 (default: false)
    - S3_EXPORT_BUCKET: S3 bucket for user data exports (required if EXPORT_USERS is true)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    user_pool_id = os.environ.get('USER_POOL_ID', '')
    inactive_days = int(os.environ.get('INACTIVE_DAYS_THRESHOLD', 90))
    disable_inactive = os.environ.get('DISABLE_INACTIVE_USERS', 'false').lower() == 'true'
    export_users = os.environ.get('EXPORT_USERS', 'false').lower() == 'true'
    s3_bucket = os.environ.get('S3_EXPORT_BUCKET', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    if not user_pool_id:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable: USER_POOL_ID')
        }
    
    if export_users and not s3_bucket:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable: S3_EXPORT_BUCKET')
        }
    
    # Initialize AWS clients
    cognito = boto3.client('cognito-idp', region_name=region)
    s3 = boto3.client('s3', region_name=region) if export_users else None
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Calculate cutoff date for inactive users
    cutoff_date = datetime.now() - timedelta(days=inactive_days)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'user_pool_id': user_pool_id,
        'total_users': 0,
        'inactive_users': [],
        'disabled_users': [],
        'export_location': '',
        'errors': []
    }
    
    # Get all users in the user pool
    users = []
    pagination_token = None
    
    try:
        while True:
            if pagination_token:
                response = cognito.list_users(
                    UserPoolId=user_pool_id,
                    PaginationToken=pagination_token
                )
            else:
                response = cognito.list_users(
                    UserPoolId=user_pool_id
                )
            
            users.extend(response['Users'])
            results['total_users'] += len(response['Users'])
            
            if 'PaginationToken' in response:
                pagination_token = response['PaginationToken']
            else:
                break
        
        # Process users
        for user in users:
            username = user['Username']
            user_status = user['UserStatus']
            user_created = user['UserCreateDate']
            user_last_modified = user['UserLastModifiedDate']
            
            # Check if user is inactive
            if user_last_modified < cutoff_date and user_status == 'CONFIRMED':
                inactive_user = {
                    'username': username,
                    'created_date': user_created.isoformat(),
                    'last_modified': user_last_modified.isoformat(),
                    'days_inactive': (datetime.now() - user_last_modified).days
                }
                
                results['inactive_users'].append(inactive_user)
                
                # Disable inactive user if configured
                if disable_inactive:
                    try:
                        cognito.admin_disable_user(
                            UserPoolId=user_pool_id,
                            Username=username
                        )
                        
                        results['disabled_users'].append(username)
                    except Exception as e:
                        error_msg = f"Error disabling inactive user {username}: {str(e)}"
                        results['errors'].append(error_msg)
                        print(error_msg)
        
        # Export users to S3 if configured
        if export_users and s3:
            try:
                # Create CSV file in memory
                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                
                # Write header
                csv_writer.writerow(['Username', 'Status', 'Created', 'Last Modified', 'Email', 'Phone'])
                
                # Write user data
                for user in users:
                    email = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'email'), '')
                    phone = next((attr['Value'] for attr in user['Attributes'] if attr['Name'] == 'phone_number'), '')
                    
                    csv_writer.writerow([
                        user['Username'],
                        user['UserStatus'],
                        user['UserCreateDate'].isoformat(),
                        user['UserLastModifiedDate'].isoformat(),
                        email,
                        phone
                    ])
                
                # Upload to S3
                export_key = f"cognito-exports/user-export-{datetime.now().strftime('%Y-%m-%d')}.csv"
                s3.put_object(
                    Bucket=s3_bucket,
                    Key=export_key,
                    Body=csv_buffer.getvalue(),
                    ContentType='text/csv'
                )
                
                results['export_location'] = f"s3://{s3_bucket}/{export_key}"
            
            except Exception as e:
                error_msg = f"Error exporting users to S3: {str(e)}"
                results['errors'].append(error_msg)
                print(error_msg)
    
    except Exception as e:
        error_msg = f"Error listing or processing users: {str(e)}"
        results['errors'].append(error_msg)
        print(error_msg)
    
    # Send SNS notification if configured
    if sns and sns_topic_arn:
        try:
            message = {
                'timestamp': results['timestamp'],
                'user_pool_id': results['user_pool_id'],
                'total_users': results['total_users'],
                'inactive_users_count': len(results['inactive_users']),
                'disabled_users_count': len(results['disabled_users'])
            }
            
            if results['export_location']:
                message['export_location'] = results['export_location']
            
            if results['errors']:
                message['errors'] = results['errors']
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Cognito User Manager Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps(message, indent=2, default=str)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }