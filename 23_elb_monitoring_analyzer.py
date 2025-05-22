import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor and analyze Elastic Load Balancers.
    
    This function monitors ELB/ALB/NLB metrics and configurations:
    - Identifies load balancers with high error rates
    - Detects load balancers with unhealthy targets
    - Analyzes traffic patterns and distribution
    - Checks for security and configuration best practices
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - ERROR_RATE_THRESHOLD: Error rate threshold percentage (default: 5)
    - UNHEALTHY_TARGET_THRESHOLD: Percentage of unhealthy targets to alert on (default: 20)
    - LOOKBACK_HOURS: Hours of metrics to analyze (default: 24)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    error_rate_threshold = float(os.environ.get('ERROR_RATE_THRESHOLD', 5))
    unhealthy_target_threshold = float(os.environ.get('UNHEALTHY_TARGET_THRESHOLD', 20))
    lookback_hours = int(os.environ.get('LOOKBACK_HOURS', 24))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    elbv2 = boto3.client('elbv2', region_name=region)
    elb = boto3.client('elb', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Calculate time range for metrics
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=lookback_hours)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'high_error_rate_load_balancers': [],
        'unhealthy_target_groups': [],
        'security_issues': [],
        'configuration_recommendations': []
    }
    
    # Get all Application and Network Load Balancers
    alb_nlb_load_balancers = []
    try:
        paginator = elbv2.get_paginator('describe_load_balancers')
        for page in paginator.paginate():
            alb_nlb_load_balancers.extend(page['LoadBalancers'])
    except Exception as e:
        print(f"Error getting ALB/NLB load balancers: {str(e)}")
    
    # Get all Classic Load Balancers
    classic_load_balancers = []
    try:
        paginator = elb.get_paginator('describe_load_balancers')
        for page in paginator.paginate():
            classic_load_balancers.extend(page['LoadBalancerDescriptions'])
    except Exception as e:
        print(f"Error getting Classic load balancers: {str(e)}")
    
    # Analyze Application and Network Load Balancers
    for lb in alb_nlb_load_balancers:
        lb_arn = lb['LoadBalancerArn']
        lb_name = lb['LoadBalancerName']
        lb_type = lb['Type']
        
        # Check for security issues
        if lb_type == 'application':
            # Check if HTTPS listeners are using outdated policies
            listeners = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
            for listener in listeners['Listeners']:
                if listener.get('Protocol') == 'HTTPS':
                    # Check SSL policy
                    ssl_policy = listener.get('SslPolicy', '')
                    if ssl_policy and ssl_policy in ['ELBSecurityPolicy-2016-08', 'ELBSecurityPolicy-TLS-1-0-2015-04']:
                        results['security_issues'].append({
                            'load_balancer_name': lb_name,
                            'load_balancer_type': lb_type,
                            'issue_type': 'outdated_ssl_policy',
                            'details': f"Using outdated SSL policy: {ssl_policy}"
                        })
        
        # Get target groups for this load balancer
        target_groups_response = elbv2.describe_target_groups(LoadBalancerArn=lb_arn)
        
        for target_group in target_groups_response['TargetGroups']:
            target_group_arn = target_group['TargetGroupArn']
            target_group_name = target_group['TargetGroupName']
            
            # Check target health
            target_health = elbv2.describe_target_health(TargetGroupArn=target_group_arn)
            
            total_targets = len(target_health['TargetHealthDescriptions'])
            unhealthy_targets = sum(1 for target in target_health['TargetHealthDescriptions'] 
                                  if target['TargetHealth']['State'] != 'healthy')
            
            if total_targets > 0:
                unhealthy_percentage = (unhealthy_targets / total_targets) * 100
                
                if unhealthy_percentage >= unhealthy_target_threshold:
                    results['unhealthy_target_groups'].append({
                        'load_balancer_name': lb_name,
                        'target_group_name': target_group_name,
                        'total_targets': total_targets,
                        'unhealthy_targets': unhealthy_targets,
                        'unhealthy_percentage': unhealthy_percentage
                    })
            
            # Check for high error rates (5XX errors)
            if lb_type == 'application':
                try:
                    response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ApplicationELB',
                        MetricName='HTTPCode_Target_5XX_Count',
                        Dimensions=[
                            {'Name': 'LoadBalancer', 'Value': lb_arn.split('/')[-2] + '/' + lb_arn.split('/')[-1]},
                            {'Name': 'TargetGroup', 'Value': target_group_arn.split(':')[-1]}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,
                        Statistics=['Sum']
                    )
                    
                    # Get request count for the same period
                    request_count_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/ApplicationELB',
                        MetricName='RequestCount',
                        Dimensions=[
                            {'Name': 'LoadBalancer', 'Value': lb_arn.split('/')[-2] + '/' + lb_arn.split('/')[-1]},
                            {'Name': 'TargetGroup', 'Value': target_group_arn.split(':')[-1]}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,
                        Statistics=['Sum']
                    )
                    
                    # Calculate error rate
                    if request_count_response['Datapoints'] and response['Datapoints']:
                        total_errors = sum(point['Sum'] for point in response['Datapoints'])
                        total_requests = sum(point['Sum'] for point in request_count_response['Datapoints'])
                        
                        if total_requests > 0:
                            error_rate = (total_errors / total_requests) * 100
                            
                            if error_rate >= error_rate_threshold:
                                results['high_error_rate_load_balancers'].append({
                                    'load_balancer_name': lb_name,
                                    'target_group_name': target_group_name,
                                    'error_rate': error_rate,
                                    'total_errors': total_errors,
                                    'total_requests': total_requests
                                })
                except Exception as e:
                    print(f"Error getting metrics for {lb_name}/{target_group_name}: {str(e)}")
    
    # Analyze Classic Load Balancers
    for lb in classic_load_balancers:
        lb_name = lb['LoadBalancerName']
        
        # Check for security issues
        try:
            policies = elb.describe_load_balancer_policies(LoadBalancerName=lb_name)
            for policy in policies['PolicyDescriptions']:
                if policy['PolicyTypeName'] == 'SSLNegotiationPolicyType':
                    # Check for outdated protocols
                    for attribute in policy['PolicyAttributeDescriptions']:
                        if attribute['AttributeName'] in ['Protocol-TLSv1', 'Protocol-SSLv3'] and attribute['AttributeValue'] == 'true':
                            results['security_issues'].append({
                                'load_balancer_name': lb_name,
                                'load_balancer_type': 'classic',
                                'issue_type': 'outdated_protocol',
                                'details': f"Using outdated protocol: {attribute['AttributeName']}"
                            })
        except Exception as e:
            print(f"Error checking policies for {lb_name}: {str(e)}")
        
        # Check for high error rates (5XX errors)
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/ELB',
                MetricName='HTTPCode_Backend_5XX',
                Dimensions=[
                    {'Name': 'LoadBalancerName', 'Value': lb_name}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )
            
            # Get request count for the same period
            request_count_response = cloudwatch.get_metric_statistics(
                Namespace='AWS/ELB',
                MetricName='RequestCount',
                Dimensions=[
                    {'Name': 'LoadBalancerName', 'Value': lb_name}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )
            
            # Calculate error rate
            if request_count_response['Datapoints'] and response['Datapoints']:
                total_errors = sum(point['Sum'] for point in response['Datapoints'])
                total_requests = sum(point['Sum'] for point in request_count_response['Datapoints'])
                
                if total_requests > 0:
                    error_rate = (total_errors / total_requests) * 100
                    
                    if error_rate >= error_rate_threshold:
                        results['high_error_rate_load_balancers'].append({
                            'load_balancer_name': lb_name,
                            'load_balancer_type': 'classic',
                            'error_rate': error_rate,
                            'total_errors': total_errors,
                            'total_requests': total_requests
                        })
        except Exception as e:
            print(f"Error getting metrics for {lb_name}: {str(e)}")
        
        # Check instance health
        try:
            instance_health = elb.describe_instance_health(LoadBalancerName=lb_name)
            
            total_instances = len(instance_health['InstanceStates'])
            unhealthy_instances = sum(1 for instance in instance_health['InstanceStates'] 
                                    if instance['State'] != 'InService')
            
            if total_instances > 0:
                unhealthy_percentage = (unhealthy_instances / total_instances) * 100
                
                if unhealthy_percentage >= unhealthy_target_threshold:
                    results['unhealthy_target_groups'].append({
                        'load_balancer_name': lb_name,
                        'load_balancer_type': 'classic',
                        'total_instances': total_instances,
                        'unhealthy_instances': unhealthy_instances,
                        'unhealthy_percentage': unhealthy_percentage
                    })
        except Exception as e:
            print(f"Error checking instance health for {lb_name}: {str(e)}")
    
    # Generate configuration recommendations
    for lb in alb_nlb_load_balancers:
        lb_name = lb['LoadBalancerName']
        lb_type = lb['Type']
        
        # Check if access logs are enabled
        if lb_type == 'application':
            try:
                attributes = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb['LoadBalancerArn'])
                access_logs_enabled = False
                
                for attr in attributes['Attributes']:
                    if attr['Key'] == 'access_logs.s3.enabled' and attr['Value'] == 'true':
                        access_logs_enabled = True
                
                if not access_logs_enabled:
                    results['configuration_recommendations'].append({
                        'load_balancer_name': lb_name,
                        'load_balancer_type': lb_type,
                        'recommendation': 'enable_access_logs',
                        'details': 'Enable access logs to track requests for security and troubleshooting'
                    })
            except Exception as e:
                print(f"Error checking attributes for {lb_name}: {str(e)}")
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn and (results['high_error_rate_load_balancers'] or 
                                 results['unhealthy_target_groups'] or 
                                 results['security_issues']):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"ELB Monitoring Alert - {end_time.strftime('%Y-%m-%d')}",
                Message=json.dumps(results, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }