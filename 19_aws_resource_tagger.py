import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to automatically tag AWS resources.
    
    This function helps maintain consistent tagging across AWS resources:
    - Tags newly created resources based on event notifications
    - Enforces mandatory tags on resources
    - Updates tags on existing resources
    - Generates compliance reports for tag policies
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - DEFAULT_TAGS: JSON string of default tags to apply (default: {"ManagedBy": "Lambda"})
    - ENFORCE_TAGS: Whether to enforce mandatory tags (default: false)
    - MANDATORY_TAGS: Comma-separated list of mandatory tag keys (default: "Name,Environment")
    - TAG_REPORT_BUCKET: S3 bucket for tag compliance reports (optional)
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    default_tags_str = os.environ.get('DEFAULT_TAGS', '{"ManagedBy": "Lambda"}')
    enforce_tags = os.environ.get('ENFORCE_TAGS', 'false').lower() == 'true'
    mandatory_tags_str = os.environ.get('MANDATORY_TAGS', 'Name,Environment')
    tag_report_bucket = os.environ.get('TAG_REPORT_BUCKET')
    
    # Parse default tags
    try:
        default_tags = json.loads(default_tags_str)
    except json.JSONDecodeError:
        default_tags = {"ManagedBy": "Lambda"}
    
    # Parse mandatory tags
    mandatory_tags = [tag.strip() for tag in mandatory_tags_str.split(',')]
    
    # Initialize AWS clients
    ec2 = boto3.client('ec2', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    rds = boto3.client('rds', region_name=region)
    dynamodb = boto3.client('dynamodb', region_name=region)
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Add creation timestamp to default tags
    default_tags['CreatedAt'] = datetime.now().isoformat()
    
    # Process CloudTrail events
    resources_tagged = []
    resources_missing_tags = []
    errors = []
    
    if 'detail' in event and 'eventName' in event['detail']:
        event_name = event['detail']['eventName']
        
        # Handle EC2 instance creation
        if event_name == 'RunInstances':
            try:
                instance_ids = []
                for item in event['detail']['responseElements']['instancesSet']['items']:
                    instance_ids.append(item['instanceId'])
                
                if instance_ids:
                    # Convert default tags to EC2 format
                    ec2_tags = [{'Key': k, 'Value': v} for k, v in default_tags.items()]
                    
                    # Tag instances
                    ec2.create_tags(
                        Resources=instance_ids,
                        Tags=ec2_tags
                    )
                    
                    resources_tagged.extend([
                        {'resource_type': 'ec2:instance', 'resource_id': instance_id}
                        for instance_id in instance_ids
                    ])
                    
                    # Check for mandatory tags if enforcing
                    if enforce_tags:
                        for instance_id in instance_ids:
                            response = ec2.describe_tags(
                                Filters=[
                                    {'Name': 'resource-id', 'Values': [instance_id]}
                                ]
                            )
                            
                            existing_tag_keys = [tag['Key'] for tag in response['Tags']]
                            missing_tags = [tag for tag in mandatory_tags if tag not in existing_tag_keys]
                            
                            if missing_tags:
                                resources_missing_tags.append({
                                    'resource_type': 'ec2:instance',
                                    'resource_id': instance_id,
                                    'missing_tags': missing_tags
                                })
            except Exception as e:
                errors.append({
                    'operation': 'tag_ec2_instances',
                    'error': str(e)
                })
        
        # Handle S3 bucket creation
        elif event_name == 'CreateBucket':
            try:
                bucket_name = event['detail']['requestParameters']['bucketName']
                
                # Convert default tags to S3 format
                s3_tags = {'TagSet': [{'Key': k, 'Value': v} for k, v in default_tags.items()]}
                
                # Tag bucket
                s3.put_bucket_tagging(
                    Bucket=bucket_name,
                    Tagging=s3_tags
                )
                
                resources_tagged.append({
                    'resource_type': 's3:bucket',
                    'resource_id': bucket_name
                })
                
                # Check for mandatory tags if enforcing
                if enforce_tags:
                    response = s3.get_bucket_tagging(Bucket=bucket_name)
                    
                    existing_tag_keys = [tag['Key'] for tag in response['TagSet']]
                    missing_tags = [tag for tag in mandatory_tags if tag not in existing_tag_keys]
                    
                    if missing_tags:
                        resources_missing_tags.append({
                            'resource_type': 's3:bucket',
                            'resource_id': bucket_name,
                            'missing_tags': missing_tags
                        })
            except Exception as e:
                errors.append({
                    'operation': 'tag_s3_bucket',
                    'error': str(e)
                })
        
        # Handle RDS instance creation
        elif event_name == 'CreateDBInstance':
            try:
                db_instance_id = event['detail']['requestParameters']['dBInstanceIdentifier']
                
                # Convert default tags to RDS format
                rds_tags = [{'Key': k, 'Value': v} for k, v in default_tags.items()]
                
                # Tag DB instance
                rds.add_tags_to_resource(
                    ResourceName=f"arn:aws:rds:{region}:{event['account']}:db:{db_instance_id}",
                    Tags=rds_tags
                )
                
                resources_tagged.append({
                    'resource_type': 'rds:db',
                    'resource_id': db_instance_id
                })
                
                # Check for mandatory tags if enforcing
                if enforce_tags:
                    response = rds.list_tags_for_resource(
                        ResourceName=f"arn:aws:rds:{region}:{event['account']}:db:{db_instance_id}"
                    )
                    
                    existing_tag_keys = [tag['Key'] for tag in response['TagList']]
                    missing_tags = [tag for tag in mandatory_tags if tag not in existing_tag_keys]
                    
                    if missing_tags:
                        resources_missing_tags.append({
                            'resource_type': 'rds:db',
                            'resource_id': db_instance_id,
                            'missing_tags': missing_tags
                        })
            except Exception as e:
                errors.append({
                    'operation': 'tag_rds_instance',
                    'error': str(e)
                })
    
    # Generate tag compliance report if bucket is configured
    if tag_report_bucket:
        try:
            # Prepare report data
            report = {
                'timestamp': datetime.now().isoformat(),
                'resources_tagged': resources_tagged,
                'resources_missing_tags': resources_missing_tags,
                'errors': errors
            }
            
            # Upload report to S3
            timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            s3.put_object(
                Bucket=tag_report_bucket,
                Key=f"tag-reports/{timestamp}.json",
                Body=json.dumps(report, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            errors.append({
                'operation': 'generate_tag_report',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Resource tagging completed',
            'timestamp': datetime.now().isoformat(),
            'resources_tagged': resources_tagged,
            'resources_missing_tags': resources_missing_tags,
            'errors': errors
        })
    }