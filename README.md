


          
# AWS Lambda Scripts Collection

This repository contains a collection of AWS Lambda functions designed to automate various AWS service management tasks. These scripts provide monitoring, optimization, security, and maintenance capabilities across a wide range of AWS services.

## Overview

The collection includes 40 Lambda functions that can be deployed individually or as part of a comprehensive AWS management solution. Each script is designed to address specific operational needs within AWS environments.

## Scripts Included

### Compute Services
- **01_ec2_instance_scheduler.py**: Automatically starts and stops EC2 instances based on tags and schedules
- **09_ecs_autoscaling_manager.py**: Manages ECS service autoscaling based on CloudWatch metrics
- **25_elastic_beanstalk_monitor.py**: Monitors Elastic Beanstalk environments for health issues
- **30_aws_emr_cluster_manager.py**: Manages EMR clusters including creation, monitoring, and termination
- **37_aws_batch_job_monitor.py**: Monitors AWS Batch jobs and job queues

### Storage Services
- **02_s3_bucket_cleanup.py**: Cleans up S3 buckets by deleting objects older than specified retention periods
- **04_rds_snapshot_manager.py**: Creates and manages RDS database snapshots
- **07_dynamodb_backup_manager.py**: Manages DynamoDB table backups
- **17_ebs_volume_snapshot_manager.py**: Creates and manages EBS volume snapshots
- **33_aws_neptune_backup_manager.py**: Manages Amazon Neptune database backups

### Monitoring & Logging
- **03_cloudwatch_logs_exporter.py**: Exports CloudWatch logs to S3 for archiving
- **08_lambda_function_monitor.py**: Monitors Lambda functions for errors and performance issues
- **12_cloudtrail_event_analyzer.py**: Analyzes CloudTrail events for suspicious activities
- **14_route53_health_check_monitor.py**: Monitors Route53 health checks and DNS records
- **15_backup_verification.py**: Verifies AWS Backup jobs and recovery points
- **23_elb_monitoring_analyzer.py**: Monitors ELB/ALB/NLB metrics and configurations
- **27_step_functions_monitor.py**: Monitors AWS Step Functions executions
- **28_aws_glue_job_monitor.py**: Monitors AWS Glue ETL jobs
- **38_aws_sagemaker_model_monitor.py**: Monitors SageMaker models and endpoints

### Security & Compliance
- **05_security_group_auditor.py**: Audits security groups for risky configurations
- **10_iam_access_analyzer.py**: Analyzes IAM permissions and identifies security risks
- **11_vpc_flow_logs_analyzer.py**: Analyzes VPC Flow Logs for security insights
- **13_config_compliance_checker.py**: Checks AWS Config compliance status
- **22_acm_certificate_expiry_monitor.py**: Monitors ACM certificate expiration
- **24_secrets_manager_rotation.py**: Manages rotation of secrets in AWS Secrets Manager
- **26_secrets_manager_rotation.py**: Alternative implementation for Secrets Manager rotation
- **32_kms_key_rotation_manager.py**: Manages KMS key rotation
- **39_aws_guardduty_findings_processor.py**: Processes and responds to GuardDuty findings
- **40_aws_ssm_patch_compliance_reporter.py**: Reports on Systems Manager patch compliance

### Cost Optimization
- **06_cost_optimization_advisor.py**: Identifies cost optimization opportunities
- **29_aws_athena_query_optimizer.py**: Optimizes AWS Athena queries

### API & Integration
- **16_cloudfront_cache_invalidator.py**: Invalidates CloudFront cache when content changes
- **18_sqs_dead_letter_queue_processor.py**: Processes messages in SQS Dead Letter Queues
- **20_api_gateway_usage_analyzer.py**: Analyzes API Gateway usage patterns
- **31_eventbridge_rule_manager.py**: Manages EventBridge rules
- **34_aws_elasticsearch_index_manager.py**: Manages Amazon Elasticsearch/OpenSearch indices
- **35_aws_cognito_user_manager.py**: Manages AWS Cognito user pools
- **36_aws_transfer_family_monitor.py**: Monitors AWS Transfer Family services

### Governance & Management
- **19_aws_resource_tagger.py**: Automatically tags AWS resources
- **21_organizations_policy_analyzer.py**: Analyzes AWS Organizations policies

## Usage

Each Lambda function is designed to be deployed independently. The functions use environment variables for configuration, allowing for easy customization without code changes.

### Deployment

1. Create a new Lambda function in the AWS Management Console
2. Upload the Python script or copy/paste the code
3. Configure the appropriate IAM role with necessary permissions
4. Set the required environment variables
5. Configure the trigger (CloudWatch Events, S3, etc.)

### Environment Variables

Each script uses environment variables for configuration. Common variables include:

- `REGION`: AWS region to operate in (default: us-east-1)
- `SNS_TOPIC_ARN`: Optional SNS topic ARN for notifications
- Script-specific configuration parameters (documented in each script)

## Requirements

- Python 3.8+
- AWS SDK for Python (boto3)
- IAM roles with appropriate permissions for each Lambda function

## Best Practices

- Review and customize IAM permissions for each Lambda function
- Set appropriate memory and timeout values based on workload
- Use CloudWatch Alarms to monitor Lambda function errors
- Consider using AWS Lambda Layers for common dependencies
- Test thoroughly in a non-production environment before deployment

## Contributing

Contributions to improve existing scripts or add new functionality are welcome. Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
