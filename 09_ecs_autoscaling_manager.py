import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to manage ECS service autoscaling.
    
    This function configures and adjusts autoscaling for ECS services based on
    CloudWatch metrics and custom scaling policies.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - CLUSTER_SERVICES: JSON mapping of cluster names to service names
      Format: '{"cluster1": ["service1", "service2"], "cluster2": ["service3"]}'
    - MIN_CAPACITY: Minimum number of tasks (default: 1)
    - MAX_CAPACITY: Maximum number of tasks (default: 10)
    - CPU_SCALE_OUT_THRESHOLD: CPU percentage to trigger scale out (default: 75)
    - CPU_SCALE_IN_THRESHOLD: CPU percentage to trigger scale in (default: 25)
    - MEMORY_SCALE_OUT_THRESHOLD: Memory percentage to trigger scale out (default: 75)
    - MEMORY_SCALE_IN_THRESHOLD: Memory percentage to trigger scale in (default: 25)
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    cluster_services_json = os.environ.get('CLUSTER_SERVICES', '{}')
    min_capacity = int(os.environ.get('MIN_CAPACITY', 1))
    max_capacity = int(os.environ.get('MAX_CAPACITY', 10))
    cpu_scale_out = int(os.environ.get('CPU_SCALE_OUT_THRESHOLD', 75))
    cpu_scale_in = int(os.environ.get('CPU_SCALE_IN_THRESHOLD', 25))
    memory_scale_out = int(os.environ.get('MEMORY_SCALE_OUT_THRESHOLD', 75))
    memory_scale_in = int(os.environ.get('MEMORY_SCALE_IN_THRESHOLD', 25))
    
    # Parse cluster services JSON
    try:
        cluster_services = json.loads(cluster_services_json)
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid CLUSTER_SERVICES JSON format')
        }
    
    # Initialize AWS clients
    ecs = boto3.client('ecs', region_name=region)
    appautoscaling = boto3.client('application-autoscaling', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    results = {
        'configured_services': [],
        'errors': []
    }
    
    for cluster_name, services in cluster_services.items():
        for service_name in services:
            try:
                # Register scalable target
                resource_id = f"service/{cluster_name}/{service_name}"
                
                try:
                    appautoscaling.register_scalable_target(
                        ServiceNamespace='ecs',
                        ResourceId=resource_id,
                        ScalableDimension='ecs:service:DesiredCount',
                        MinCapacity=min_capacity,
                        MaxCapacity=max_capacity
                    )
                except appautoscaling.exceptions.ValidationException:
                    # Target might already be registered, try to update it
                    appautoscaling.register_scalable_target(
                        ServiceNamespace='ecs',
                        ResourceId=resource_id,
                        ScalableDimension='ecs:service:DesiredCount',
                        MinCapacity=min_capacity,
                        MaxCapacity=max_capacity
                    )
                
                # Configure CPU scale-out policy
                cpu_scale_out_policy = appautoscaling.put_scaling_policy(
                    PolicyName=f"{service_name}-cpu-scale-out",
                    ServiceNamespace='ecs',
                    ResourceId=resource_id,
                    ScalableDimension='ecs:service:DesiredCount',
                    PolicyType='StepScaling',
                    StepScalingPolicyConfiguration={
                        'AdjustmentType': 'ChangeInCapacity',
                        'StepAdjustments': [
                            {
                                'MetricIntervalLowerBound': 0,
                                'ScalingAdjustment': 1
                            }
                        ],
                        'Cooldown': 300
                    }
                )
                
                # Configure CPU scale-in policy
                cpu_scale_in_policy = appautoscaling.put_scaling_policy(
                    PolicyName=f"{service_name}-cpu-scale-in",
                    ServiceNamespace='ecs',
                    ResourceId=resource_id,
                    ScalableDimension='ecs:service:DesiredCount',
                    PolicyType='StepScaling',
                    StepScalingPolicyConfiguration={
                        'AdjustmentType': 'ChangeInCapacity',
                        'StepAdjustments': [
                            {
                                'MetricIntervalUpperBound': 0,
                                'ScalingAdjustment': -1
                            }
                        ],
                        'Cooldown': 300
                    }
                )
                
                # Create CPU scale-out alarm
                cloudwatch.put_metric_alarm(
                    AlarmName=f"{service_name}-cpu-high",
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=2,
                    MetricName='CPUUtilization',
                    Namespace='AWS/ECS',
                    Period=60,
                    Statistic='Average',
                    Threshold=cpu_scale_out,
                    AlarmDescription=f'Alarm when CPU exceeds {cpu_scale_out}%',
                    Dimensions=[
                        {
                            'Name': 'ClusterName',
                            'Value': cluster_name
                        },
                        {
                            'Name': 'ServiceName',
                            'Value': service_name
                        }
                    ],
                    AlarmActions=[cpu_scale_out_policy['PolicyARN']]
                )
                
                # Create CPU scale-in alarm
                cloudwatch.put_metric_alarm(
                    AlarmName=f"{service_name}-cpu-low",
                    ComparisonOperator='LessThanThreshold',
                    EvaluationPeriods=2,
                    MetricName='CPUUtilization',
                    Namespace='AWS/ECS',
                    Period=60,
                    Statistic='Average',
                    Threshold=cpu_scale_in,
                    AlarmDescription=f'Alarm when CPU is below {cpu_scale_in}%',
                    Dimensions=[
                        {
                            'Name': 'ClusterName',
                            'Value': cluster_name
                        },
                        {
                            'Name': 'ServiceName',
                            'Value': service_name
                        }
                    ],
                    AlarmActions=[cpu_scale_in_policy['PolicyARN']]
                )
                
                # Configure Memory scale-out policy
                memory_scale_out_policy = appautoscaling.put_scaling_policy(
                    PolicyName=f"{service_name}-memory-scale-out",
                    ServiceNamespace='ecs',
                    ResourceId=resource_id,
                    ScalableDimension='ecs:service:DesiredCount',
                    PolicyType='StepScaling',
                    StepScalingPolicyConfiguration={
                        'AdjustmentType': 'ChangeInCapacity',
                        'StepAdjustments': [
                            {
                                'MetricIntervalLowerBound': 0,
                                'ScalingAdjustment': 1
                            }
                        ],
                        'Cooldown': 300
                    }
                )
                
                # Configure Memory scale-in policy
                memory_scale_in_policy = appautoscaling.put_scaling_policy(
                    PolicyName=f"{service_name}-memory-scale-in",
                    ServiceNamespace='ecs',
                    ResourceId=resource_id,
                    ScalableDimension='ecs:service:DesiredCount',
                    PolicyType='StepScaling',
                    StepScalingPolicyConfiguration={
                        'AdjustmentType': 'ChangeInCapacity',
                        'StepAdjustments': [
                            {
                                'MetricIntervalUpperBound': 0,
                                'ScalingAdjustment': -1
                            }
                        ],
                        'Cooldown': 300
                    }
                )
                
                # Create Memory scale-out alarm
                cloudwatch.put_metric_alarm(
                    AlarmName=f"{service_name}-memory-high",
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=2,
                    MetricName='MemoryUtilization',
                    Namespace='AWS/ECS',
                    Period=60,
                    Statistic='Average',
                    Threshold=memory_scale_out,
                    AlarmDescription=f'Alarm when Memory exceeds {memory_scale_out}%',
                    Dimensions=[
                        {
                            'Name': 'ClusterName',
                            'Value': cluster_name
                        },
                        {
                            'Name': 'ServiceName',
                            'Value': service_name
                        }
                    ],
                    AlarmActions=[memory_scale_out_policy['PolicyARN']]
                )
                
                # Create Memory scale-in alarm
                cloudwatch.put_metric_alarm(
                    AlarmName=f"{service_name}-memory-low",
                    ComparisonOperator='LessThanThreshold',
                    EvaluationPeriods=2,
                    MetricName='MemoryUtilization',
                    Namespace='AWS/ECS',
                    Period=60,
                    Statistic='Average',
                    Threshold=memory_scale_in,
                    AlarmDescription=f'Alarm when Memory is below {memory_scale_in}%',
                    Dimensions=[
                        {
                            'Name': 'ClusterName',
                            'Value': cluster_name
                        },
                        {
                            'Name': 'ServiceName',
                            'Value': service_name
                        }
                    ],
                    AlarmActions=[memory_scale_in_policy['PolicyARN']]
                )
                
                results['configured_services'].append({
                    'cluster': cluster_name,
                    'service': service_name,
                    'min_capacity': min_capacity,
                    'max_capacity': max_capacity,
                    'cpu_thresholds': {
                        'scale_out': cpu_scale_out,
                        'scale_in': cpu_scale_in
                    },
                    'memory_thresholds': {
                        'scale_out': memory_scale_out,
                        'scale_in': memory_scale_in
                    }
                })
                
            except Exception as e:
                results['errors'].append({
                    'cluster': cluster_name,
                    'service': service_name,
                    'error': str(e)
                })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'ECS autoscaling configuration completed',
            'results': results
        })
    }