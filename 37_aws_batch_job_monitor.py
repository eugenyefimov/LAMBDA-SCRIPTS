import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor AWS Batch jobs.
    
    This function monitors AWS Batch jobs and job queues:
    - Tracks job success/failure rates
    - Identifies stuck or long-running jobs
    - Monitors compute environment utilization
    - Provides recommendations for queue and compute environment optimization
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - JOB_QUEUES: Optional comma-separated list of job queue ARNs to monitor
    - LOOKBACK_HOURS: Hours of job history to analyze (default: 24)
    - LONG_RUNNING_THRESHOLD_HOURS: Threshold for long-running jobs in hours (default: 2)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    job_queues_str = os.environ.get('JOB_QUEUES', '')
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    long_running_threshold = int(os.environ.get('LONG_RUNNING_THRESHOLD_HOURS', 2))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    batch = boto3.client('batch', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Calculate time range for analysis
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'job_queues_analyzed': [],
        'job_statistics': {},
        'failed_jobs': [],
        'stuck_jobs': [],
        'long_running_jobs': [],
        'recommendations': []
    }
    
    # Get list of job queues
    job_queues = []
    if job_queues_str:
        job_queues = [queue.strip() for queue in job_queues_str.split(',')]
    else:
        try:
            response = batch.describe_job_queues()
            job_queues = [queue['jobQueueArn'] for queue in response['jobQueues']]
        except Exception as e:
            print(f"Error listing job queues: {str(e)}")
    
    # Analyze each job queue
    for queue_arn in job_queues:
        try:
            # Get queue details
            queue_details = batch.describe_job_queues(jobQueues=[queue_arn])
            if not queue_details['jobQueues']:
                continue
                
            queue_info = queue_details['jobQueues'][0]
            queue_name = queue_info['jobQueueName']
            
            queue_stats = {
                'queue_name': queue_name,
                'queue_arn': queue_arn,
                'state': queue_info['state'],
                'priority': queue_info['priority'],
                'total_jobs': 0,
                'succeeded_jobs': 0,
                'failed_jobs': 0,
                'running_jobs': 0,
                'pending_jobs': 0
            }
            
            # Get compute environments for this queue
            compute_envs = [ce['computeEnvironment'] for ce in queue_info['computeEnvironmentOrder']]
            
            # List jobs in this queue
            job_list = []
            
            # Get RUNNING jobs
            try:
                running_response = batch.list_jobs(
                    jobQueue=queue_arn,
                    jobStatus='RUNNING'
                )
                job_list.extend(running_response['jobSummaryList'])
                queue_stats['running_jobs'] = len(running_response['jobSummaryList'])
                
                # Check for long-running jobs
                for job in running_response['jobSummaryList']:
                    started_at = job.get('startedAt', 0) / 1000  # Convert from milliseconds
                    if started_at > 0:
                        job_duration = datetime.now() - datetime.fromtimestamp(started_at)
                        if job_duration > timedelta(hours=long_running_threshold):
                            results['long_running_jobs'].append({
                                'job_id': job['jobId'],
                                'job_name': job['jobName'],
                                'queue': queue_name,
                                'duration_hours': job_duration.total_seconds() / 3600,
                                'started_at': datetime.fromtimestamp(started_at).isoformat()
                            })
            except Exception as e:
                print(f"Error listing running jobs for queue {queue_name}: {str(e)}")
            
            # Get PENDING jobs
            try:
                pending_response = batch.list_jobs(
                    jobQueue=queue_arn,
                    jobStatus='PENDING'
                )
                job_list.extend(pending_response['jobSummaryList'])
                queue_stats['pending_jobs'] = len(pending_response['jobSummaryList'])
                
                # Check for stuck jobs
                for job in pending_response['jobSummaryList']:
                    created_at = job.get('createdAt', 0) / 1000  # Convert from milliseconds
                    if created_at > 0:
                        pending_duration = datetime.now() - datetime.fromtimestamp(created_at)
                        if pending_duration > timedelta(hours=1):  # Pending for more than 1 hour
                            results['stuck_jobs'].append({
                                'job_id': job['jobId'],
                                'job_name': job['jobName'],
                                'queue': queue_name,
                                'pending_hours': pending_duration.total_seconds() / 3600,
                                'created_at': datetime.fromtimestamp(created_at).isoformat()
                            })
            except Exception as e:
                print(f"Error listing pending jobs for queue {queue_name}: {str(e)}")
            
            # Get SUCCEEDED jobs in the lookback period
            try:
                succeeded_response = batch.list_jobs(
                    jobQueue=queue_arn,
                    jobStatus='SUCCEEDED',
                    filters=[
                        {
                            'name': 'AFTER_CREATED_AT',
                            'values': [str(int(start_time.timestamp() * 1000))]
                        }
                    ]
                )
                job_list.extend(succeeded_response['jobSummaryList'])
                queue_stats['succeeded_jobs'] = len(succeeded_response['jobSummaryList'])
            except Exception as e:
                print(f"Error listing succeeded jobs for queue {queue_name}: {str(e)}")
            
            # Get FAILED jobs in the lookback period
            try:
                failed_response = batch.list_jobs(
                    jobQueue=queue_arn,
                    jobStatus='FAILED',
                    filters=[
                        {
                            'name': 'AFTER_CREATED_AT',
                            'values': [str(int(start_time.timestamp() * 1000))]
                        }
                    ]
                )
                job_list.extend(failed_response['jobSummaryList'])
                queue_stats['failed_jobs'] = len(failed_response['jobSummaryList'])
                
                # Add failed jobs to results
                for job in failed_response['jobSummaryList']:
                    # Get job details to find reason for failure
                    job_details = batch.describe_jobs(jobs=[job['jobId']])
                    if job_details['jobs']:
                        job_detail = job_details['jobs'][0]
                        results['failed_jobs'].append({
                            'job_id': job['jobId'],
                            'job_name': job['jobName'],
                            'queue': queue_name,
                            'created_at': datetime.fromtimestamp(job['createdAt'] / 1000).isoformat(),
                            'reason': job_detail.get('statusReason', 'Unknown')
                        })
            except Exception as e:
                print(f"Error listing failed jobs for queue {queue_name}: {str(e)}")
            
            queue_stats['total_jobs'] = (
                queue_stats['running_jobs'] + 
                queue_stats['pending_jobs'] + 
                queue_stats['succeeded_jobs'] + 
                queue_stats['failed_jobs']
            )
            
            # Calculate success rate
            if queue_stats['succeeded_jobs'] + queue_stats['failed_jobs'] > 0:
                success_rate = (
                    queue_stats['succeeded_jobs'] / 
                    (queue_stats['succeeded_jobs'] + queue_stats['failed_jobs']) * 100
                )
            else:
                success_rate = 100
                
            queue_stats['success_rate'] = success_rate
            
            # Add queue stats to results
            results['job_queues_analyzed'].append(queue_stats)
            results['job_statistics'][queue_name] = {
                'total_jobs': queue_stats['total_jobs'],
                'success_rate': success_rate,
                'running_jobs': queue_stats['running_jobs'],
                'pending_jobs': queue_stats['pending_jobs']
            }
            
            # Generate recommendations
            if queue_stats['pending_jobs'] > queue_stats['running_jobs'] * 2:
                results['recommendations'].append({
                    'queue': queue_name,
                    'recommendation': 'Consider scaling up compute resources to handle pending job backlog',
                    'severity': 'Medium'
                })
                
            if success_rate < 80:
                results['recommendations'].append({
                    'queue': queue_name,
                    'recommendation': 'Investigate high failure rate in this queue',
                    'severity': 'High'
                })
                
            if len(results['stuck_jobs']) > 0:
                results['recommendations'].append({
                    'queue': queue_name,
                    'recommendation': 'Check for resource constraints or issues with compute environments',
                    'severity': 'High'
                })
                
        except Exception as e:
            print(f"Error analyzing job queue {queue_arn}: {str(e)}")
    
    # Send SNS notification if configured
    if sns and sns_topic_arn and (results['failed_jobs'] or results['stuck_jobs']):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"AWS Batch Job Monitor - Issues Detected",
                Message=json.dumps({
                    'timestamp': results['timestamp'],
                    'failed_jobs_count': len(results['failed_jobs']),
                    'stuck_jobs_count': len(results['stuck_jobs']),
                    'long_running_jobs_count': len(results['long_running_jobs']),
                    'recommendations': results['recommendations']
                }, indent=2, default=str)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }