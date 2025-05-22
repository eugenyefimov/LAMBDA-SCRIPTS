import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Data Pipeline status.
    
    This function:
    - Lists pipelines and checks their health and status.
    - Can be configured to check specific pipelines or all pipelines.
    - Sends notifications for unhealthy or failed pipelines.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - PIPELINE_IDS: Comma-separated list of pipeline IDs to monitor (optional, default: all pipelines).
    - SNS_TOPIC_ARN: SNS topic ARN for notifications of pipeline issues.
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    pipeline_ids_str = os.environ.get('PIPELINE_IDS', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    if not sns_topic_arn:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variable: SNS_TOPIC_ARN')
        }

    # Initialize AWS clients
    datapipeline_client = boto3.client('datapipeline', region_name=region)
    sns = boto3.client('sns', region_name=region)
    
    pipelines_to_check = [pid.strip() for pid in pipeline_ids_str.split(',')] if pipeline_ids_str else []
    
    monitor_results = {
        'timestamp': datetime.now().isoformat(),
        'problem_pipelines': [],
        'checked_pipelines_count': 0,
        'errors': []
    }

    try:
        if not pipelines_to_check:
            # List all pipelines if specific IDs are not provided
            paginator = datapipeline_client.get_paginator('list_pipelines')
            for page in paginator.paginate():
                for pipeline_id_name in page.get('pipelineIdList', []):
                    pipelines_to_check.append(pipeline_id_name['id'])
        
        monitor_results['checked_pipelines_count'] = len(pipelines_to_check)

        for pipeline_id in pipelines_to_check:
            try:
                description_response = datapipeline_client.describe_pipelines(pipelineIds=[pipeline_id])
                
                if not description_response.get('pipelineDescriptionList'):
                    monitor_results['errors'].append(f"Pipeline ID {pipeline_id} not found or no description returned.")
                    continue

                pipeline_desc = description_response['pipelineDescriptionList'][0]
                pipeline_name = pipeline_desc['name']
                
                # Check health status and state from fields
                # Example: Iterate through fields to find '@healthStatus' and '@pipelineState'
                health_status = None
                pipeline_state = None
                
                for field in pipeline_desc.get('fields', []):
                    if field['key'] == '@healthStatus':
                        health_status = field['stringValue']
                    elif field['key'] == '@pipelineState':
                        pipeline_state = field['stringValue']
                
                is_problem = False
                problem_details = []

                if health_status and health_status != 'HEALTHY':
                    is_problem = True
                    problem_details.append(f"Health Status: {health_status}")
                
                # Common problematic states: ERROR, FAILED, TIMEDOUT, CANCELED (depending on definition of "problem")
                problematic_states = ['ERROR', 'FAILED', 'TIMEDOUT'] 
                if pipeline_state and pipeline_state in problematic_states:
                    is_problem = True
                    problem_details.append(f"Pipeline State: {pipeline_state}")

                if is_problem:
                    problem_info = {
                        'PipelineId': pipeline_id,
                        'PipelineName': pipeline_name,
                        'Details': ", ".join(problem_details)
                    }
                    monitor_results['problem_pipelines'].append(problem_info)
                    
                    message = f"AWS Data Pipeline Alert for {pipeline_name} (ID: {pipeline_id}):\n"
                    message += f"  Problem: {problem_info['Details']}\n"
                    message += f"  Check the AWS Data Pipeline console for more details."
                    sns.publish(
                        TopicArn=sns_topic_arn,
                        Message=message,
                        Subject=f"Data Pipeline Issue: {pipeline_name}"
                    )

            except Exception as e_pipeline:
                error_msg = f"Error processing pipeline {pipeline_id}: {str(e_pipeline)}"
                monitor_results['errors'].append(error_msg)
                sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject="Data Pipeline Monitor Error")


    except Exception as e:
        error_message = f"Error in Data Pipeline Monitor: {str(e)}"
        monitor_results['errors'].append(error_message)
        sns.publish(TopicArn=sns_topic_arn, Message=error_message, Subject="Data Pipeline Monitor System Error")
        return {
            'statusCode': 500,
            'body': json.dumps(monitor_results)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(monitor_results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    # os.environ['PIPELINE_IDS'] = 'df-EXAMPLE_PIPELINE_ID_1,df-EXAMPLE_PIPELINE_ID_2'
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace with your SNS Topic
    # Note: Data Pipeline API calls might require specific permissions and active pipelines to test effectively.
    print(lambda_handler({}, {}))