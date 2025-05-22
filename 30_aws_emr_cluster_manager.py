import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage Amazon EMR clusters.
    
    This function automates EMR cluster management:
    - Creates clusters based on predefined configurations
    - Monitors running clusters for utilization and errors
    - Terminates idle or completed clusters
    - Optimizes cluster configurations based on usage patterns
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - CLUSTER_CONFIGS: S3 path to JSON file with cluster configurations
    - IDLE_THRESHOLD_HOURS: Hours of idle time before terminating a cluster (default: 1)
    - MAX_CLUSTER_LIFETIME_HOURS: Maximum lifetime for clusters in hours (default: 24)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    cluster_configs_path = os.environ.get('CLUSTER_CONFIGS', '')
    idle_threshold_hours = int(os.environ.get('IDLE_THRESHOLD_HOURS', 1))
    max_cluster_lifetime = int(os.environ.get('MAX_CLUSTER_LIFETIME_HOURS', 24))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    emr = boto3.client('emr', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'clusters_created': [],
        'clusters_terminated': [],
        'clusters_monitored': [],
        'optimization_recommendations': []
    }
    
    # Load cluster configurations if provided
    cluster_configs = []
    if cluster_configs_path:
        try:
            if cluster_configs_path.startswith('s3://'):
                # Parse S3 URL
                parts = cluster_configs_path.replace('s3://', '').split('/')
                bucket = parts[0]
                key = '/'.join(parts[1:])
                
                response = s3.get_object(Bucket=bucket, Key=key)
                cluster_configs = json.loads(response['Body'].read().decode('utf-8'))
            else:
                # Assume it's a local file path
                with open(cluster_configs_path, 'r') as f:
                    cluster_configs = json.load(f)
        except Exception as e:
            print(f"Error loading cluster configurations: {str(e)}")
    
    # Create clusters based on configurations
    for config in cluster_configs:
        try:
            # Check if we should create this cluster (based on schedule, etc.)
            should_create = True
            
            # Check if there's a schedule
            if 'schedule' in config:
                schedule = config['schedule']
                current_day = datetime.now().strftime('%a').lower()
                current_hour = int(datetime.now().strftime('%H'))
                
                # Check if current day is in schedule
                if 'days' in schedule and current_day not in [d.lower() for d in schedule['days'].split(',')]:
                    should_create = False
                
                # Check if current hour is in schedule
                if 'hours' in schedule:
                    hours = [int(h.strip()) for h in schedule['hours'].split(',')]
                    if current_hour not in hours:
                        should_create = False
            
            if should_create:
                # Create the cluster
                response = emr.run_job_flow(**config['cluster_params'])
                
                results['clusters_created'].append({
                    'cluster_id': response['JobFlowId'],
                    'name': config['cluster_params'].get('Name', 'Unnamed cluster'),
                    'creation_time': datetime.now().isoformat()
                })
        except Exception as e:
            print(f"Error creating cluster: {str(e)}")
    
    # Monitor running clusters
    try:
        paginator = emr.get_paginator('list_clusters')
        for page in paginator.paginate(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']):
            for cluster in page['Clusters']:
                cluster_id = cluster['Id']
                cluster_name = cluster['Name']
                
                # Get cluster details
                cluster_details = emr.describe_cluster(ClusterId=cluster_id)
                
                # Check cluster age
                creation_time = cluster_details['Cluster']['Status']['Timeline'].get('CreationDateTime')
                if creation_time:
                    cluster_age = datetime.now() - creation_time.replace(tzinfo=None)
                    
                    # Check if cluster has exceeded maximum lifetime
                    if cluster_age > timedelta(hours=max_cluster_lifetime):
                        # Terminate the cluster
                        emr.terminate_job_flows(JobFlowIds=[cluster_id])
                        
                        results['clusters_terminated'].append({
                            'cluster_id': cluster_id,
                            'name': cluster_name,
                            'reason': 'Exceeded maximum lifetime',
                            'age_hours': cluster_age.total_seconds() / 3600
                        })
                        continue
                
                # Check if cluster is idle
                is_idle = False
                
                # Get cluster metrics
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=idle_threshold_hours)
                
                # Check YARN memory available
                try:
                    memory_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ElasticMapReduce',
                        MetricName='YARNMemoryAvailablePercentage',
                        Dimensions=[
                            {'Name': 'JobFlowId', 'Value': cluster_id}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=300,  # 5-minute periods
                        Statistics=['Average']
                    )
                    
                    # If memory available is consistently high, cluster might be idle
                    if memory_response['Datapoints']:
                        avg_memory_available = sum(dp['Average'] for dp in memory_response['Datapoints']) / len(memory_response['Datapoints'])
                        if avg_memory_available > 90:  # More than 90% memory available
                            is_idle = True
                except Exception as e:
                    print(f"Error getting memory metrics for cluster {cluster_id}: {str(e)}")
                
                # Check for active steps
                steps_response = emr.list_steps(
                    ClusterId=cluster_id,
                    StepStates=['PENDING', 'RUNNING']
                )
                
                if not steps_response['Steps']:
                    # No active steps
                    is_idle = is_idle and True
                else:
                    is_idle = False
                
                if is_idle:
                    # Terminate the idle cluster
                    emr.terminate_job_flows(JobFlowIds=[cluster_id])
                    
                    results['clusters_terminated'].append({
                        'cluster_id': cluster_id,
                        'name': cluster_name,
                        'reason': 'Idle cluster',
                        'idle_hours': idle_threshold_hours
                    })
                else:
                    # Add to monitored clusters
                    results['clusters_monitored'].append({
                        'cluster_id': cluster_id,
                        'name': cluster_name,
                        'status': cluster['Status']['State'],
                        'creation_time': creation_time.isoformat() if creation_time else None
                    })
    except Exception as e:
        print(f"Error monitoring clusters: {str(e)}")
    
    # Generate optimization recommendations
    try:
        # Get terminated clusters from the last 7 days
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
        
        paginator = emr.get_paginator('list_clusters')
        for page in paginator.paginate(
            CreatedAfter=start_time,
            CreatedBefore=end_time,
            ClusterStates=['TERMINATED']
        ):
            for cluster in page['Clusters']:
                cluster_id = cluster['Id']
                
                # Get cluster details
                cluster_details = emr.describe_cluster(ClusterId=cluster_id)
                
                # Check if cluster was short-lived (less than 1 hour)
                timeline = cluster_details['Cluster']['Status']['Timeline']
                if 'CreationDateTime' in timeline and 'EndDateTime' in timeline:
                    duration = timeline['EndDateTime'] - timeline['CreationDateTime']
                    if duration.total_seconds() < 3600:  # Less than 1 hour
                        # This might be a candidate for Spot instances or transient clusters
                        results['optimization_recommendations'].append({
                            'cluster_id': cluster_id,
                            'name': cluster['Name'],
                            'recommendation': 'Consider using Spot instances for short-lived clusters',
                            'duration_minutes': duration.total_seconds() / 60
                        })
    except Exception as e:
        print(f"Error generating optimization recommendations: {str(e)}")
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn and (results['clusters_created'] or results['clusters_terminated']):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"EMR Cluster Management Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps({
                    'clusters_created': results['clusters_created'],
                    'clusters_terminated': results['clusters_terminated'],
                    'optimization_recommendations': results['optimization_recommendations']
                }, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }