import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to manage AWS Systems Manager Parameter Store parameters.
    
    This function can:
    - Create or update parameters.
    - Delete parameters.
    - Be triggered by events or run on a schedule to ensure parameters are up-to-date.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - PARAMETER_NAME: The name of the parameter to manage.
    - PARAMETER_VALUE: The value to set for the parameter (for create/update).
    - PARAMETER_TYPE: The type of parameter (String, StringList, SecureString, default: String).
    - PARAMETER_ACTION: Action to perform (PUT, DELETE, default: PUT).
    - PARAMETER_DESCRIPTION: Optional description for the parameter.
    - PARAMETER_OVERWRITE: Boolean (true/false) to overwrite an existing parameter (default: false for PUT).
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications.
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    parameter_name = os.environ.get('PARAMETER_NAME')
    parameter_value = os.environ.get('PARAMETER_VALUE')
    parameter_type = os.environ.get('PARAMETER_TYPE', 'String')
    parameter_action = os.environ.get('PARAMETER_ACTION', 'PUT').upper()
    parameter_description = os.environ.get('PARAMETER_DESCRIPTION', '')
    parameter_overwrite_str = os.environ.get('PARAMETER_OVERWRITE', 'false').lower()
    parameter_overwrite = parameter_overwrite_str == 'true'
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    if not parameter_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable: PARAMETER_NAME')
        }
    
    if parameter_action == 'PUT' and parameter_value is None: # Value can be empty string, but not None
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable for PUT action: PARAMETER_VALUE')
        }

    # Initialize AWS clients
    ssm_client = boto3.client('ssm', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'parameter_name': parameter_name,
        'action_taken': parameter_action,
        'status': '',
        'message': ''
    }

    try:
        if parameter_action == 'PUT':
            ssm_client.put_parameter(
                Name=parameter_name,
                Description=parameter_description,
                Value=parameter_value,
                Type=parameter_type,
                Overwrite=parameter_overwrite
            )
            results['status'] = 'Success'
            results['message'] = f"Parameter '{parameter_name}' was successfully put."
            if sns:
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Message=results['message'],
                    Subject=f"SSM Parameter Store Update: {parameter_name}"
                )

        elif parameter_action == 'DELETE':
            ssm_client.delete_parameter(Name=parameter_name)
            results['status'] = 'Success'
            results['message'] = f"Parameter '{parameter_name}' was successfully deleted."
            if sns:
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Message=results['message'],
                    Subject=f"SSM Parameter Store Deletion: {parameter_name}"
                )
        else:
            results['status'] = 'Error'
            results['message'] = f"Invalid PARAMETER_ACTION: {parameter_action}. Must be PUT or DELETE."
            return {
                'statusCode': 400,
                'body': json.dumps(results)
            }

    except ssm_client.exceptions.ParameterAlreadyExists as e:
        results['status'] = 'Error'
        results['message'] = f"Error updating parameter '{parameter_name}': Parameter already exists and overwrite is false. {str(e)}"
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=results['message'], Subject=f"SSM Parameter Store Update Error: {parameter_name}")
        return {
            'statusCode': 409, # Conflict
            'body': json.dumps(results)
        }
    except ssm_client.exceptions.ParameterNotFound as e:
        results['status'] = 'Error'
        results['message'] = f"Error deleting parameter '{parameter_name}': Parameter not found. {str(e)}"
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=results['message'], Subject=f"SSM Parameter Store Deletion Error: {parameter_name}")
        return {
            'statusCode': 404, # Not Found
            'body': json.dumps(results)
        }
    except Exception as e:
        results['status'] = 'Error'
        results['message'] = f"Error processing parameter '{parameter_name}': {str(e)}"
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=results['message'], Subject=f"SSM Parameter Store Error: {parameter_name}")
        return {
            'statusCode': 500,
            'body': json.dumps(results)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['PARAMETER_NAME'] = '/my-app/config/db-host'
    os.environ['PARAMETER_VALUE'] = 'new.database.host.com'
    os.environ['PARAMETER_TYPE'] = 'String'
    os.environ['PARAMETER_ACTION'] = 'PUT'
    os.environ['PARAMETER_OVERWRITE'] = 'true'
    # os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic'
    print(lambda_handler({}, {}))

    # Example for DELETE
    # os.environ['PARAMETER_ACTION'] = 'DELETE'
    # print(lambda_handler({}, {}))