import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Glue ETL jobs.
    
    This function monitors AWS Glue jobs:
    - Tracks job success/failure rates
    - Identifies failed jobs and their causes
    - Monitors job duration and resource utilization
    - Alerts on abnormal patterns
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - JOB_NAMES: Optional comma-separated list of Glue job names to monitor
    - LOOKBACK_DAYS: Days of job history to analyze (default: 7)
    - FAILURE_THRESHOLD: Percentage threshold for high failure rates (default: 10)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    job_names_str = os.environ.get('JOB_NAMES', '')
    lookback_days = int(os.environ.get('LOOKBACK_DAYS', 7))
    failure_threshold = float(os.environ.get('FAILURE_THRESHOLD', 10))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    glue = boto3.client('glue', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Parse job names
    job_names = [name.strip() for name in job_names_str.split(',')] if job_names_str else []
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=lookback_days)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'jobs_checked': 0,
        'total_runs': 0,
        'successful_runs': 0,
        'failed_runs': 0,
        'job_metrics': [],
        'problematic_jobs': [],
        'recent_failures': []
    }
    
    # If no job names provided, list all jobs
    if not job_names:
        try:
            paginator = glue.get_paginator('get_jobs')
            for page in paginator.paginate():
                for job in page['Jobs']:
                    job_names.append(job['Name'])
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error listing Glue jobs',
                    'error': str(e)
                })
            }
    
    # Analyze each job
    for job_name in job_names:
        try:
            job_metrics = {
                'name': job_name,
                'runs_analyzed': 0,
                'successful_runs': 0,
                'failed_runs': 0,
                'average_duration_seconds': 0,
                'average_dpu_seconds': 0,
                'recent_runs': []
            }
            
            # Get job runs
            total_duration = 0
            total_dpu_seconds = 0
            
            paginator = glue.get_paginator('get_job_runs')
            for page in paginator.paginate(
                JobName=job_name,
                StartedAfter=start_time
            ):
                for run in page['JobRuns']:
                    job_metrics['runs_analyzed'] += 1
                    results['total_runs'] += 1
                    
                    # Calculate duration
                    start_time = run.get('StartedOn')
                    end_time = run.get('CompletedOn')
                    
                    if start_time and end_time:
                        duration = (end_time - start_time).total_seconds()
                        total_duration += duration
                    else:
                        duration = None
                    
                    # Get DPU seconds if available
                    dpu_seconds = run.get('AllocatedCapacity', 0) * duration if duration else 0
                    total_dpu_seconds += dpu_seconds
                    
                    # Check run status
                    if run['JobRunState'] in ['SUCCEEDED']:
                        job_metrics['successful_runs'] += 1
                        results['successful_runs'] += 1
                    elif run['JobRunState'] in ['FAILED', 'TIMEOUT', 'ERROR']:
                        job_metrics['failed_runs'] += 1
                        results['failed_runs'] += 1
                        
                        # Add to recent failures
                        results['recent_failures'].append({
                            'job_name': job_name,
                            'run_id': run['Id'],
                            'state': run['JobRunState'],
                            'error_message': run.get('ErrorMessage', ''),
                            'start_time': run.get('StartedOn', '').isoformat() if run.get('StartedOn') else None,
                            'end_time': run.get('CompletedOn', '').isoformat() if run.get('CompletedOn') else None
                        })
                    
                    # Add to recent runs
                    job_metrics['recent_runs'].append({
                        'run_id': run['Id'],
                        'state': run['JobRunState'],
                        'start_time': run.get('StartedOn', '').isoformat() if run.get('StartedOn') else None,
                        'end_time': run.get('CompletedOn', '').isoformat() if run.get('CompletedOn') else None,
                        'duration_seconds': duration,
                        'dpu_seconds': dpu_seconds
                    })
            
            # Calculate averages
            if job_metrics['runs_analyzed'] > 0:
                job_metrics['average_duration_seconds'] = total_duration / job_metrics['runs_analyzed']
                job_metrics['average_dpu_seconds'] = total_dpu_seconds / job_metrics['runs_analyzed']
            
            # Calculate failure rate
            if job_metrics['runs_analyzed'] > 0:
                failure_rate = (job_metrics['failed_runs'] / job_metrics['runs_analyzed']) * 100
                job_metrics['failure_rate'] = failure_rate
                
                # Check if job is problematic
                if failure_rate >= failure_threshold:
                    results['problematic_jobs'].append({
                        'job_name': job_name,
                        'failure_rate': failure_rate,
                        'runs_analyzed': job_metrics['runs_analyzed'],
                        'failed_runs': job_metrics['failed_runs']
                    })
            
            results['job_metrics'].append(job_metrics)
            results['jobs_checked'] += 1
            
        except Exception as e:
            print(f"Error analyzing job {job_name}: {str(e)}")
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn and results['problematic_jobs']:
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"AWS Glue Job Monitoring Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps({
                    'problematic_jobs': results['problematic_jobs'],
                    'recent_failures': results['recent_failures']
                }, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }