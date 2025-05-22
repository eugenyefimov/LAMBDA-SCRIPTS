import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to start or stop EC2 instances based on tags.
    
    This function looks for EC2 instances with specific scheduling tags:
    - 'AutoStart': Contains the time to start instances (format: HH:MM)
    - 'AutoStop': Contains the time to stop instances (format: HH:MM)
    - 'AutoStartDays': Contains days to start (format: Mon,Tue,Wed,Thu,Fri,Sat,Sun)
    - 'AutoStopDays': Contains days to stop (format: Mon,Tue,Wed,Thu,Fri,Sat,Sun)
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    """
    # Get AWS region from environment variable or use default
    region = os.environ.get('REGION', 'us-east-1')
    ec2 = boto3.resource('ec2', region_name=region)
    
    # Get current time and day
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    current_day = now.strftime("%a")
    
    instances_started = []
    instances_stopped = []
    
    # Find instances with scheduling tags
    instances = ec2.instances.filter(
        Filters=[
            {'Name': 'tag-key', 'Values': ['AutoStart', 'AutoStop']}
        ]
    )
    
    for instance in instances:
        instance_id = instance.id
        tags = {tag['Key']: tag['Value'] for tag in instance.tags or []}
        
        # Check for start time
        if 'AutoStart' in tags and 'AutoStartDays' in tags:
            start_time = tags['AutoStart']
            start_days = tags['AutoStartDays'].split(',')
            
            if current_time == start_time and current_day in start_days and instance.state['Name'] == 'stopped':
                instance.start()
                instances_started.append(instance_id)
        
        # Check for stop time
        if 'AutoStop' in tags and 'AutoStopDays' in tags:
            stop_time = tags['AutoStop']
            stop_days = tags['AutoStopDays'].split(',')
            
            if current_time == stop_time and current_day in stop_days and instance.state['Name'] == 'running':
                instance.stop()
                instances_stopped.append(instance_id)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'instances_started': instances_started,
            'instances_stopped': instances_stopped
        })
    }