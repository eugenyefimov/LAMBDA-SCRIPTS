import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to check AWS Config compliance status.
    
    This function analyzes AWS Config rules compliance and reports non-compliant resources.
    It can also trigger remediation actions for specific non-compliant resources.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - AUTO_REMEDIATE: Whether to trigger auto-remediation (default: false)
    - REMEDIATION_RULES: Comma-separated list of rule names to auto-remediate
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    auto_remediate = os.environ.get('AUTO_REMEDIATE', 'false').lower() == 'true'
    remediation_rules_str = os.environ.get('REMEDIATION_RULES', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Parse remediation rules
    remediation_rules = [rule.strip() for rule in remediation_rules_str.split(',')] if remediation_rules_str else []
    
    # Initialize AWS clients
    config = boto3.client('config', region_name=region)
    
    # Get compliance status for all rules
    compliance_by_rule = {}
    non_compliant_resources = {}
    
    paginator = config.get_paginator('describe_compliance_by_config_rule')
    for page in paginator.paginate(
        ComplianceTypes=['NON_COMPLIANT']
    ):
        for rule in page['ComplianceByConfigRules']:
            rule_name = rule['ConfigRuleName']
            compliance_by_rule[rule_name] = rule['Compliance']
            
            # Get non-compliant resources for this rule
            resource_paginator = config.get_paginator('get_compliance_details_by_config_rule')
            resources = []
            
            for resource_page in resource_paginator.paginate(
                ConfigRuleName=rule_name,
                ComplianceTypes=['NON_COMPLIANT']
            ):
                resources.extend(resource_page['EvaluationResults'])
            
            non_compliant_resources[rule_name] = resources
    
    # Trigger remediation if enabled
    remediation_results = {}
    if auto_remediate:
        for rule_name in remediation_rules:
            if rule_name in non_compliant_resources:
                remediation_results[rule_name] = []
                
                for evaluation in non_compliant_resources[rule_name]:
                    resource_id = evaluation['EvaluationResultIdentifier']['EvaluationResultQualifier']['ResourceId']
                    resource_type = evaluation['EvaluationResultIdentifier']['EvaluationResultQualifier']['ResourceType']
                    
                    try:
                        response = config.start_remediation_execution(
                            ConfigRuleName=rule_name,
                            ResourceKeys=[
                                {
                                    'ResourceType': resource_type,
                                    'ResourceId': resource_id
                                }
                            ]
                        )
                        
                        remediation_results[rule_name].append({
                            'resource_id': resource_id,
                            'resource_type': resource_type,
                            'status': 'Remediation started',
                            'execution_id': response['FailedItems'][0]['FailureMessage'] if response['FailedItems'] else 'Success'
                        })
                    except Exception as e:
                        remediation_results[rule_name].append({
                            'resource_id': resource_id,
                            'resource_type': resource_type,
                            'status': 'Remediation failed',
                            'error': str(e)
                        })
    
    # Prepare summary
    summary = {
        'total_rules': len(compliance_by_rule),
        'non_compliant_rules': len(non_compliant_resources),
        'total_non_compliant_resources': sum(len(resources) for resources in non_compliant_resources.values()),
        'remediation_attempted': len(remediation_results) if auto_remediate else 0
    }
    
    # Send to SNS if configured
    if sns_topic_arn and summary['non_compliant_rules'] > 0:
        sns = boto3.client('sns', region_name=region)
        
        message = {
            'subject': f"AWS Config Compliance - {summary['non_compliant_rules']} non-compliant rules",
            'timestamp': datetime.now().isoformat(),
            'summary': summary,
            'non_compliant_resources': non_compliant_resources,
            'remediation_results': remediation_results if auto_remediate else 'Auto-remediation disabled'
        }
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(message, indent=2),
            Subject=message['subject']
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'AWS Config compliance check completed',
            'summary': summary,
            'non_compliant_resources': non_compliant_resources,
            'remediation_results': remediation_results if auto_remediate else 'Auto-remediation disabled'
        })
    }