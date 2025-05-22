import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage EventBridge rules.
    
    This function automates EventBridge rule management:
    - Creates rules based on predefined patterns
    - Enables/disables rules based on schedules
    - Monitors rule execution history
    - Cleans up unused or outdated rules
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - RULE_DEFINITIONS: S3 path to JSON file with rule definitions
    - CLEANUP_UNUSED_DAYS: Days of inactivity before marking a rule for cleanup (default: 90)
    - DRY_RUN: If true, only report changes without making them (default: false)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    rule_definitions_path = os.environ.get('RULE_DEFINITIONS', '')
    cleanup_unused_days = int(os.environ.get('CLEANUP_UNUSED_DAYS', 90))
    dry_run = os.environ.get('DRY_RUN', 'false').lower() == 'true'
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    events = boto3.client('events', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'rules_created': [],
        'rules_updated': [],
        'rules_enabled': [],
        'rules_disabled': [],
        'rules_deleted': [],
        'errors': []
    }
    
    # Load rule definitions if provided
    rule_definitions = []
    if rule_definitions_path:
        try:
            if rule_definitions_path.startswith('s3://'):
                # Parse S3 URL
                parts = rule_definitions_path.replace('s3://', '').split('/')
                bucket = parts[0]
                key = '/'.join(parts[1:])
                
                response = s3.get_object(Bucket=bucket, Key=key)
                rule_definitions = json.loads(response['Body'].read().decode('utf-8'))
            else:
                # Assume it's a local file path
                with open(rule_definitions_path, 'r') as f:
                    rule_definitions = json.load(f)
        except Exception as e:
            error_msg = f"Error loading rule definitions: {str(e)}"
            results['errors'].append(error_msg)
            print(error_msg)
    
    # Create or update rules based on definitions
    for rule_def in rule_definitions:
        try:
            rule_name = rule_def.get('Name')
            if not rule_name:
                results['errors'].append("Rule definition missing required 'Name' field")
                continue
                
            # Check if rule exists
            try:
                existing_rule = events.describe_rule(Name=rule_name)
                rule_exists = True
            except events.exceptions.ResourceNotFoundException:
                rule_exists = False
            
            # Create or update rule
            if not rule_exists:
                if not dry_run:
                    response = events.put_rule(
                        Name=rule_name,
                        ScheduleExpression=rule_def.get('ScheduleExpression', ''),
                        EventPattern=rule_def.get('EventPattern', ''),
                        State=rule_def.get('State', 'ENABLED'),
                        Description=rule_def.get('Description', ''),
                        RoleArn=rule_def.get('RoleArn', ''),
                        Tags=rule_def.get('Tags', [])
                    )
                    
                    # Add targets if specified
                    if 'Targets' in rule_def:
                        events.put_targets(
                            Rule=rule_name,
                            Targets=rule_def['Targets']
                        )
                
                results['rules_created'].append(rule_name)
            else:
                # Update existing rule
                if not dry_run:
                    response = events.put_rule(
                        Name=rule_name,
                        ScheduleExpression=rule_def.get('ScheduleExpression', existing_rule.get('ScheduleExpression', '')),
                        EventPattern=rule_def.get('EventPattern', existing_rule.get('EventPattern', '')),
                        State=rule_def.get('State', existing_rule.get('State', 'ENABLED')),
                        Description=rule_def.get('Description', existing_rule.get('Description', '')),
                        RoleArn=rule_def.get('RoleArn', existing_rule.get('RoleArn', ''))
                    )
                    
                    # Update targets if specified
                    if 'Targets' in rule_def:
                        # Remove existing targets
                        existing_targets = events.list_targets_by_rule(Rule=rule_name)
                        if existing_targets.get('Targets'):
                            target_ids = [t['Id'] for t in existing_targets['Targets']]
                            events.remove_targets(
                                Rule=rule_name,
                                Ids=target_ids
                            )
                        
                        # Add new targets
                        events.put_targets(
                            Rule=rule_name,
                            Targets=rule_def['Targets']
                        )
                
                results['rules_updated'].append(rule_name)
        except Exception as e:
            error_msg = f"Error processing rule {rule_def.get('Name', 'unknown')}: {str(e)}"
            results['errors'].append(error_msg)
            print(error_msg)
    
    # Enable/disable rules based on schedule if specified
    if 'SCHEDULE_RULES' in os.environ:
        try:
            schedule_rules = json.loads(os.environ.get('SCHEDULE_RULES', '{}'))
            current_day = datetime.now().strftime('%a').lower()
            current_hour = int(datetime.now().strftime('%H'))
            
            for rule_name, schedule in schedule_rules.items():
                try:
                    # Check if rule exists
                    try:
                        rule = events.describe_rule(Name=rule_name)
                    except events.exceptions.ResourceNotFoundException:
                        results['errors'].append(f"Rule {rule_name} not found for scheduling")
                        continue
                    
                    # Check if we should enable the rule
                    if 'enable' in schedule:
                        enable_days = [d.lower() for d in schedule['enable'].get('days', [])]
                        enable_hours = schedule['enable'].get('hours', [])
                        
                        if (not enable_days or current_day in enable_days) and \
                           (not enable_hours or current_hour in enable_hours) and \
                           rule['State'] == 'DISABLED':
                            if not dry_run:
                                events.enable_rule(Name=rule_name)
                            results['rules_enabled'].append(rule_name)
                    
                    # Check if we should disable the rule
                    if 'disable' in schedule:
                        disable_days = [d.lower() for d in schedule['disable'].get('days', [])]
                        disable_hours = schedule['disable'].get('hours', [])
                        
                        if (not disable_days or current_day in disable_days) and \
                           (not disable_hours or current_hour in disable_hours) and \
                           rule['State'] == 'ENABLED':
                            if not dry_run:
                                events.disable_rule(Name=rule_name)
                            results['rules_disabled'].append(rule_name)
                except Exception as e:
                    error_msg = f"Error scheduling rule {rule_name}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing SCHEDULE_RULES environment variable: {str(e)}"
            results['errors'].append(error_msg)
            print(error_msg)
    
    # Clean up unused rules if requested
    if cleanup_unused_days > 0:
        try:
            # Get all rules
            paginator = events.get_paginator('list_rules')
            all_rules = []
            for page in paginator.paginate():
                all_rules.extend(page['Rules'])
            
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=cleanup_unused_days)
            
            for rule in all_rules:
                rule_name = rule['Name']
                
                # Skip managed rules
                if rule_name.startswith('AWS_') or rule_name in [r.get('Name') for r in rule_definitions]:
                    continue
                
                # Check if rule has been triggered recently
                try:
                    # Check CloudWatch metrics for rule invocations
                    metric_response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/Events',
                        MetricName='Invocations',
                        Dimensions=[
                            {'Name': 'RuleName', 'Value': rule_name}
                        ],
                        StartTime=cutoff_date,
                        EndTime=datetime.now(),
                        Period=86400,  # 1 day
                        Statistics=['Sum']
                    )
                    
                    # If no data points or sum of invocations is 0, rule hasn't been triggered
                    if not metric_response['Datapoints'] or sum(dp['Sum'] for dp in metric_response['Datapoints']) == 0:
                        if not dry_run:
                            # Remove targets first
                            targets = events.list_targets_by_rule(Rule=rule_name)
                            if targets.get('Targets'):
                                target_ids = [t['Id'] for t in targets['Targets']]
                                events.remove_targets(
                                    Rule=rule_name,
                                    Ids=target_ids
                                )
                            
                            # Delete the rule
                            events.delete_rule(Name=rule_name)
                        
                        results['rules_deleted'].append(rule_name)
                except Exception as e:
                    error_msg = f"Error checking usage for rule {rule_name}: {str(e)}"
                    results['errors'].append(error_msg)
                    print(error_msg)
        except Exception as e:
            error_msg = f"Error during cleanup of unused rules: {str(e)}"
            results['errors'].append(error_msg)
            print(error_msg)
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn and (results['rules_created'] or results['rules_updated'] or 
                                 results['rules_enabled'] or results['rules_disabled'] or 
                                 results['rules_deleted'] or results['errors']):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"EventBridge Rule Manager Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps(results, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }