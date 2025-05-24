import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS CodeCommit repositories.
    
    This function can be triggered by CodeCommit events (e.g., push, branch creation)
    and perform actions like:
    - Sending notifications for specific events.
    - Triggering a CI/CD pipeline.
    - Performing automated code reviews or checks (basic).
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - SNS_TOPIC_ARN: SNS topic ARN for notifications.
    - EXPECTED_EVENT_TYPES: Comma-separated list of CodeCommit event types to process (e.g., referenceCreated,referenceUpdated).
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    expected_event_types_str = os.environ.get('EXPECTED_EVENT_TYPES', '')
    expected_event_types = [et.strip() for et in expected_event_types_str.split(',')] if expected_event_types_str else []

    if not sns_topic_arn: # Assuming notification is a primary use case
        print("Warning: SNS_TOPIC_ARN not set. Notifications will not be sent.")
    
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'processed_event': False,
        'details': {},
        'errors': []
    }

    # print("Received event:", json.dumps(event))

    try:
        # CodeCommit events are usually in event['Records'][0]['eventSource'] == 'aws:codecommit'
        # and event['Records'][0]['codecommit']['references'] gives details
        if 'Records' not in event or not event['Records']:
            results['errors'].append("Event does not contain 'Records'. Not a CodeCommit event?")
            return {'statusCode': 400, 'body': json.dumps(results)}

        for record in event['Records']:
            if record.get('eventSource') != 'aws:codecommit':
                continue

            event_name = record.get('eventName') # e.g., ReferenceChanges
            aws_region = record.get('awsRegion', region) # Region of the event
            codecommit_info = record.get('codecommit', {})
            references = codecommit_info.get('references', [])
            repository_name_from_arn = record.get('eventSourceARN', '').split(':')[-1]

            results['details']['repository_name'] = repository_name_from_arn
            results['details']['event_name'] = event_name
            results['details']['references_changed_count'] = len(references)

            for ref in references:
                reference_full_name = ref.get('ref') # e.g., refs/heads/main
                commit_id = ref.get('commit')
                reference_type = reference_full_name.split('/')[1] if '/' in reference_full_name else 'unknown' # heads, tags
                reference_name = reference_full_name.split('/')[-1]
                
                # Determine if it's a create, update, or delete based on commit ID
                # A new branch/tag will have a commit ID. A deleted one might have a zero commit or specific event type.
                # The 'eventName' (e.g. 'ReferenceChanges') and the structure of 'ref' (old vs new commit id) is key.
                # This example is simplified.
                
                action = "updated" # Default assumption
                if ref.get('created'): # Some event structures might indicate this directly
                    action = "created"
                elif ref.get('deleted'):
                    action = "deleted"
                
                # Filter by expected event types if provided
                # Note: CodeCommit eventName is often generic like 'ReferenceChanges'. 
                # You might need to inspect 'referenceType' (branch/tag) and if the commit is new/zeroed.
                # For simplicity, we'll assume any reference change is of interest if not filtered.
                
                message_detail = f"Repository: {repository_name_from_arn}\n"
                message_detail += f"Reference: {reference_full_name} ({action})\n"
                message_detail += f"Commit ID: {commit_id}\n"
                message_detail += f"User: {record.get('userIdentity', {}).get('userName', 'Unknown')}\n"
                message_detail += f"Event Time: {record.get('eventTime')}"

                results['processed_event'] = True
                results['details'].setdefault('actions', []).append(message_detail)

                if sns:
                    subject = f"CodeCommit Activity: {action.capitalize()} {reference_type} '{reference_name}' in {repository_name_from_arn}"
                    sns.publish(TopicArn=sns_topic_arn, Message=message_detail, Subject=subject)
            
            if not references:
                 results['details']['info'] = "No specific references found in CodeCommit event part."

        if not results['processed_event'] and not results['errors']:
            results['errors'].append("No CodeCommit events processed from the input.")
            return {'statusCode': 200, 'body': json.dumps(results)} # Not an error, just no relevant event

    except Exception as e:
        error_message = f"Error processing CodeCommit event: {str(e)}"
        results['errors'].append(error_message)
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=error_message, Subject="CodeCommit Monitor Error")
        return {
            'statusCode': 500,
            'body': json.dumps(results)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

if __name__ == '__main__':
    # Example usage for local testing (mock CodeCommit event)
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    example_event = {
        "Records": [
            {
                "eventSource": "aws:codecommit",
                "eventName": "ReferenceChanges",
                "awsRegion": "us-east-1",
                "eventSourceARN": "arn:aws:codecommit:us-east-1:123456789012:my-repo",
                "userIdentity": {"userName": "test-user"},
                "eventTime": datetime.now().isoformat(),
                "codecommit": {
                    "references": [
                        {
                            "ref": "refs/heads/main",
                            "commit": "abcdef1234567890"
                        },
                        {
                            "ref": "refs/tags/v1.0",
                            "commit": "fedcba0987654321",
                            "created": True
                        }
                    ]
                }
            }
        ]
    }
    print(lambda_handler(example_event, {}))