import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to detect drift in CloudFormation stacks.
    
    This function:
    - Initiates drift detection for specified CloudFormation stacks (or all stacks).
    - Checks the status of drift detection.
    - Reports stacks that have drifted from their expected template configurations.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - STACK_NAMES: Comma-separated list of stack names to check (optional, default: all stacks)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications of drifted stacks
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    stack_names_str = os.environ.get('STACK_NAMES', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    # Initialize AWS clients
    cf_client = boto3.client('cloudformation', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    stacks_to_check = [name.strip() for name in stack_names_str.split(',')] if stack_names_str else []
    
    drift_results = {
        'timestamp': datetime.now().isoformat(),
        'drifted_stacks': [],
        'checked_stacks_count': 0,
        'errors': []
    }

    try:
        if not stacks_to_check:
            # Get all stacks if specific names are not provided
            paginator = cf_client.get_paginator('list_stacks')
            for page in paginator.paginate(StackStatusFilter=[
                'CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE', 'IMPORT_COMPLETE', 'IMPORT_ROLLBACK_COMPLETE'
            ]):
                for stack_summary in page['StackSummaries']:
                    stacks_to_check.append(stack_summary['StackName'])
        
        drift_results['checked_stacks_count'] = len(stacks_to_check)

        for stack_name in stacks_to_check:
            try:
                # Initiate drift detection
                drift_detection_response = cf_client.detect_stack_drift(StackName=stack_name)
                stack_drift_detection_id = drift_detection_response['StackDriftDetectionId']
                
                # Wait for drift detection to complete (simplified for Lambda; consider Step Functions for long waits)
                # For a Lambda, you might trigger this function again or check status after a delay.
                # Here, we'll do a simple status check loop, but be mindful of Lambda timeout.
                status = ''
                max_attempts = 5 # Limit attempts to avoid long execution
                attempts = 0
                while status not in ['DETECTION_COMPLETE', 'DETECTION_FAILED'] and attempts < max_attempts:
                    attempts += 1
                    # time.sleep(10) # Be cautious with sleep in Lambda
                    drift_status_response = cf_client.describe_stack_drift_detection_status(
                        StackDriftDetectionId=stack_drift_detection_id
                    )
                    status = drift_status_response['DetectionStatus']
                    if status == 'DETECTION_IN_PROGRESS':
                        # In a real scenario, you might return here and let another trigger check later
                        pass 

                if status == 'DETECTION_COMPLETE':
                    if drift_status_response['StackDriftStatus'] == 'DRIFTED':
                        drift_results['drifted_stacks'].append({
                            'StackName': stack_name,
                            'StackDriftStatus': drift_status_response['StackDriftStatus'],
                            'DriftedStackResourceCount': drift_status_response.get('DriftedStackResourceCount', 0)
                        })
                elif status == 'DETECTION_FAILED':
                    drift_results['errors'].append(
                        f"Drift detection failed for stack {stack_name}: {drift_status_response.get('DetectionStatusReason', 'Unknown reason')}"
                    )
                else: # IN_PROGRESS after max_attempts
                     drift_results['errors'].append(
                        f"Drift detection for stack {stack_name} did not complete in time. Current status: {status}"
                    )

            except Exception as e_stack:
                drift_results['errors'].append(f"Error processing stack {stack_name}: {str(e_stack)}")

        if drift_results['drifted_stacks'] and sns:
            message = f"CloudFormation Drift Detected:\n"
            for drifted_stack in drift_results['drifted_stacks']:
                message += f"- Stack: {drifted_stack['StackName']}, Status: {drifted_stack['StackDriftStatus']}, Drifted Resources: {drifted_stack['DriftedStackResourceCount']}\n"
            sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="CloudFormation Stack Drift Alert")

    except Exception as e:
        error_message = f"Error in CloudFormation Drift Detector: {str(e)}"
        drift_results['errors'].append(error_message)
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=error_message, Subject="CloudFormation Drift Detector Error")
        
        return {
            'statusCode': 500,
            'body': json.dumps(drift_results)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(drift_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    # os.environ['STACK_NAMES'] = 'my-stack-1,my-stack-2'
    # os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic'
    print(lambda_handler({}, {}))