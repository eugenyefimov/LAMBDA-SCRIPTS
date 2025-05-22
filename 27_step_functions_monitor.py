import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Step Functions executions.
    
    This function monitors Step Functions state machines:
    - Tracks execution success/failure rates
    - Identifies failed executions and their causes
    - Monitors execution duration trends
    - Alerts on abnormal patterns
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - STATE_MACHINE_ARNS: Optional comma-separated list of state machine ARNs to monitor
    - LOOKBACK_HOURS: Hours of execution history to analyze (default: 24)
    - FAILURE_THRESHOLD: Percentage threshold for high failure rates (default: 10)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    state_machine_arns_str = os.environ.get('STATE_MACHINE_ARNS', '')
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    failure_threshold = float(os.environ.get('FAILURE_THRESHOLD', 10))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    sfn = boto3.client('stepfunctions', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Parse state machine ARNs
    state_machine_arns = [arn.strip() for arn in state_machine_arns_str.split(',')] if state_machine_arns_str else []
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'state_machines_checked': 0,
        'total_executions': 0,
        'successful_executions': 0,
        'failed_executions': 0,
        'state_machine_metrics': [],
        'problematic_state_machines': [],
        'recent_failures': []
    }
    
    # If no state machine ARNs provided, list all state machines
    if not state_machine_arns:
        try:
            paginator = sfn.get_paginator('list_state_machines')
            for page in paginator.paginate():
                for state_machine in page['stateMachines']:
                    state_machine_arns.append(state_machine['stateMachineArn'])
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing state machines',
                    'error': str(e)
                })
            }
    
    results['state_machines_checked'] = len(state_machine_arns)
    
    # Check each state machine
    for state_machine_arn in state_machine_arns:
        try:
            # Get state machine details
            state_machine = sfn.describe_state_machine(stateMachineArn=state_machine_arn)
            state_machine_name = state_machine['name']
            
            # Get executions for this state machine
            executions = []
            paginator = sfn.get_paginator('list_executions')
            for page in paginator.paginate(
                stateMachineArn=state_machine_arn,
                statusFilter='SUCCEEDED'
            ):
                for execution in page['executions']:
                    if execution['startDate'] >= start_time:
                        executions.append(execution)
            
            successful_count = len(executions)
            
            # Get failed executions
            failed_executions = []
            paginator = sfn.get_paginator('list_executions')
            for page in paginator.paginate(
                stateMachineArn=state_machine_arn,
                statusFilter='FAILED'
            ):
                for execution in page['executions']:
                    if execution['startDate'] >= start_time:
                        failed_executions.append(execution)
            
            failed_count = len(failed_executions)
            
            # Get timed out executions
            timed_out_executions = []
            paginator = sfn.get_paginator('list_executions')
            for page in paginator.paginate(
                stateMachineArn=state_machine_arn,
                statusFilter='TIMED_OUT'
            ):
                for execution in page['executions']:
                    if execution['startDate'] >= start_time:
                        timed_out_executions.append(execution)
            
            timed_out_count = len(timed_out_executions)
            
            # Get aborted executions
            aborted_executions = []
            paginator = sfn.get_paginator('list_executions')
            for page in paginator.paginate(
                stateMachineArn=state_machine_arn,
                statusFilter='ABORTED'
            ):
                for execution in page['executions']:
                    if execution['startDate'] >= start_time:
                        aborted_executions.append(execution)
            
            aborted_count = len(aborted_executions)
            
            # Calculate total and failure rate
            total_count = successful_count + failed_count + timed_out_count + aborted_count
            failure_rate = ((failed_count + timed_out_count) / total_count * 100) if total_count > 0 else 0
            
            # Calculate average execution duration for successful executions
            total_duration = sum((execution['stopDate'] - execution['startDate']).total_seconds() for execution in executions) if executions else 0
            avg_duration = total_duration / successful_count if successful_count > 0 else 0
            
            # Add to state machine metrics
            state_machine_metrics = {
                'name': state_machine_name,
                'arn': state_machine_arn,
                'total_executions': total_count,
                'successful_executions': successful_count,
                'failed_executions': failed_count,
                'timed_out_executions': timed_out_count,
                'aborted_executions': aborted_count,
                'failure_rate': failure_rate,
                'average_duration_seconds': avg_duration
            }
            
            results['state_machine_metrics'].append(state_machine_metrics)
            
            # Update totals
            results['total_executions'] += total_count
            results['successful_executions'] += successful_count
            results['failed_executions'] += failed_count + timed_out_count
            
            # Check if failure rate exceeds threshold
            if failure_rate >= failure_threshold and total_count > 0:
                results['problematic_state_machines'].append({
                    'name': state_machine_name,
                    'arn': state_machine_arn,
                    'failure_rate': failure_rate,
                    'total_executions': total_count
                })
                
                # Get details of recent failures
                recent_failures = []
                
                # Process failed executions
                for execution in failed_executions[:5]:  # Limit to 5 most recent
                    try:
                        execution_history = sfn.get_execution_history(
                            executionArn=execution['executionArn'],
                            reverseOrder=True,
                            maxResults=5  # Get the most recent events
                        )
                        
                        # Find failure event
                        failure_event = None
                        for event in execution_history['events']:
                            if event['type'].endswith('Failed') or event['type'].endswith('TimedOut'):
                                failure_event = event
                                break
                        
                        recent_failures.append({
                            'execution_arn': execution['executionArn'],
                            'start_time': execution['startDate'].isoformat(),
                            'stop_time': execution['stopDate'].isoformat() if 'stopDate' in execution else None,
                            'failure_event': failure_event
                        })
                    except Exception as e:
                        print(f"Error getting execution history for {execution['executionArn']}: {str(e)}")
                
                results['recent_failures'].extend(recent_failures)
        
        except Exception as e:
            print(f"Error processing state machine {state_machine_arn}: {str(e)}")
    
    # Send notification if there are problematic state machines and SNS topic is configured
    if results['problematic_state_machines'] and sns and sns_topic_arn:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"Step Functions Monitoring Alert - {end_time.strftime('%Y-%m-%d %H:%M')}",
                Message=json.dumps(results, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }