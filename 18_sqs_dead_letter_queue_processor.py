import boto3
import json
import os
import base64
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to process messages in SQS Dead Letter Queues.
    
    This function helps manage failed messages in SQS Dead Letter Queues:
    - Retrieves messages from DLQ
    - Logs message details for analysis
    - Optionally attempts to reprocess messages
    - Optionally archives messages to S3
    - Optionally sends notifications about failed messages
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - DLQ_URL: URL of the Dead Letter Queue to process (required)
    - MAX_MESSAGES: Maximum number of messages to process per invocation (default: 10)
    - REPROCESS_MESSAGES: Whether to attempt reprocessing (default: false)
    - TARGET_QUEUE_URL: URL of queue to send reprocessed messages to (required if reprocessing)
    - ARCHIVE_TO_S3: Whether to archive messages to S3 (default: false)
    - S3_BUCKET: S3 bucket for archiving (required if archiving)
    - S3_PREFIX: S3 prefix for archived messages (default: dlq-archive/)
    - SNS_TOPIC_ARN: SNS topic ARN for notifications (optional)
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    dlq_url = os.environ.get('DLQ_URL')
    max_messages = int(os.environ.get('MAX_MESSAGES', 10))
    reprocess_messages = os.environ.get('REPROCESS_MESSAGES', 'false').lower() == 'true'
    target_queue_url = os.environ.get('TARGET_QUEUE_URL')
    archive_to_s3 = os.environ.get('ARCHIVE_TO_S3', 'false').lower() == 'true'
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_prefix = os.environ.get('S3_PREFIX', 'dlq-archive/')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    
    # Validate required parameters
    if not dlq_url:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Missing required environment variable: DLQ_URL'
            })
        }
    
    if reprocess_messages and not target_queue_url:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Missing required environment variable for reprocessing: TARGET_QUEUE_URL'
            })
        }
    
    if archive_to_s3 and not s3_bucket:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Missing required environment variable for archiving: S3_BUCKET'
            })
        }
    
    # Initialize AWS clients
    sqs = boto3.client('sqs', region_name=region)
    s3 = boto3.client('s3', region_name=region) if archive_to_s3 else None
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Process messages
    processed_count = 0
    reprocessed_count = 0
    archived_count = 0
    errors = []
    
    while processed_count < max_messages:
        # Receive messages from DLQ
        response = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=min(10, max_messages - processed_count),
            MessageAttributeNames=['All'],
            AttributeNames=['All'],
            WaitTimeSeconds=1
        )
        
        # Break if no messages
        if 'Messages' not in response:
            break
        
        for message in response['Messages']:
            message_id = message['MessageId']
            receipt_handle = message['ReceiptHandle']
            body = message['Body']
            attributes = message.get('MessageAttributes', {})
            
            try:
                # Archive message to S3 if enabled
                if archive_to_s3:
                    timestamp = datetime.now().strftime('%Y/%m/%d/%H/%M/%S')
                    s3_key = f"{s3_prefix}{timestamp}/{message_id}.json"
                    
                    # Prepare message data for archiving
                    message_data = {
                        'MessageId': message_id,
                        'Body': body,
                        'Attributes': message.get('Attributes', {}),
                        'MessageAttributes': attributes,
                        'ArchivedAt': datetime.now().isoformat()
                    }
                    
                    # Upload to S3
                    s3.put_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Body=json.dumps(message_data, indent=2),
                        ContentType='application/json'
                    )
                    
                    archived_count += 1
                
                # Reprocess message if enabled
                if reprocess_messages:
                    # Send message to target queue
                    message_attributes = {}
                    
                    # Convert message attributes to the format expected by send_message
                    for attr_name, attr in attributes.items():
                        message_attributes[attr_name] = {
                            'DataType': attr['DataType'],
                            'StringValue': attr.get('StringValue', ''),
                            'BinaryValue': base64.b64decode(attr['BinaryValue']) if 'BinaryValue' in attr else b''
                        }
                    
                    sqs.send_message(
                        QueueUrl=target_queue_url,
                        MessageBody=body,
                        MessageAttributes=message_attributes
                    )
                    
                    reprocessed_count += 1
                
                # Delete message from DLQ
                sqs.delete_message(
                    QueueUrl=dlq_url,
                    ReceiptHandle=receipt_handle
                )
                
                processed_count += 1
                
            except Exception as e:
                errors.append({
                    'message_id': message_id,
                    'operation': 'process_message',
                    'error': str(e)
                })
    
    # Send notification if enabled
    if sns and sns_topic_arn:
        try:
            # Prepare summary
            summary = {
                'timestamp': datetime.now().isoformat(),
                'dlq_url': dlq_url,
                'processed_count': processed_count,
                'reprocessed_count': reprocessed_count,
                'archived_count': archived_count,
                'errors': len(errors)
            }
            
            # Send notification
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"SQS DLQ Processing Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                Message=json.dumps({
                    'summary': summary,
                    'details': {
                        'errors': errors
                    }
                }, indent=2)
            )
        except Exception as e:
            errors.append({
                'operation': 'send_notification',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'SQS DLQ processing completed',
            'timestamp': datetime.now().isoformat(),
            'dlq_url': dlq_url,
            'processed_count': processed_count,
            'reprocessed_count': reprocessed_count,
            'archived_count': archived_count,
            'errors': errors
        })
    }