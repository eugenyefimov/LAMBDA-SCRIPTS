import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to analyze AWS Organizations policies.
    
    This function analyzes policies across an AWS Organization:
    - Service control policies (SCPs)
    - Tag policies
    - Backup policies
    - AI services opt-out policies
    
    It identifies policy conflicts, coverage gaps, and provides recommendations.
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - POLICY_TYPES: Comma-separated list of policy types to analyze (default: SERVICE_CONTROL_POLICY,TAG_POLICY,BACKUP_POLICY,AISERVICES_OPT_OUT_POLICY)
    - REPORT_S3_BUCKET: Optional S3 bucket for storing reports
    - REPORT_S3_PREFIX: Prefix for S3 report objects (default: 'org-policy-reports/')
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    policy_types_str = os.environ.get('POLICY_TYPES', 'SERVICE_CONTROL_POLICY,TAG_POLICY,BACKUP_POLICY,AISERVICES_OPT_OUT_POLICY')
    report_bucket = os.environ.get('REPORT_S3_BUCKET', '')
    report_prefix = os.environ.get('REPORT_S3_PREFIX', 'org-policy-reports/')
    
    # Parse policy types
    policy_types = [policy_type.strip() for policy_type in policy_types_str.split(',')]
    
    # Initialize AWS clients
    org_client = boto3.client('organizations', region_name=region)
    s3_client = boto3.client('s3', region_name=region) if report_bucket else None
    
    # Get organization details
    try:
        org_details = org_client.describe_organization()
        org_id = org_details['Organization']['Id']
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error accessing AWS Organizations',
                'error': str(e)
            })
        }
    
    # Initialize results
    results = {
        'organization_id': org_id,
        'timestamp': datetime.now().isoformat(),
        'policies_analyzed': {},
        'policy_coverage': {},
        'potential_conflicts': [],
        'recommendations': []
    }
    
    # Get all organizational units
    ous = []
    paginator = org_client.get_paginator('list_organizational_units_for_parent')
    
    # Start with root
    roots = org_client.list_roots()
    root_id = roots['Roots'][0]['Id']
    
    # Add root to OUs list
    ous.append({
        'Id': root_id,
        'Name': 'Root',
        'Path': 'Root'
    })
    
    # Function to recursively get OUs
    def get_child_ous(parent_id, parent_path):
        child_ous = []
        for page in paginator.paginate(ParentId=parent_id):
            for ou in page['OrganizationalUnits']:
                ou_path = f"{parent_path}/{ou['Name']}"
                child_ous.append({
                    'Id': ou['Id'],
                    'Name': ou['Name'],
                    'Path': ou_path
                })
                # Get children of this OU
                child_ous.extend(get_child_ous(ou['Id'], ou_path))
        return child_ous
    
    # Get all OUs in the organization
    ous.extend(get_child_ous(root_id, 'Root'))
    
    # Get all accounts
    accounts = []
    paginator = org_client.get_paginator('list_accounts')
    for page in paginator.paginate():
        accounts.extend(page['Accounts'])
    
    # Analyze each policy type
    for policy_type in policy_types:
        try:
            # Get policies of this type
            policies = []
            paginator = org_client.get_paginator('list_policies')
            for page in paginator.paginate(Filter=policy_type):
                policies.extend(page['Policies'])
            
            policy_details = []
            
            for policy in policies:
                # Get policy content
                policy_detail = org_client.describe_policy(PolicyId=policy['Id'])
                
                # Get targets (OUs and accounts) for this policy
                targets = []
                paginator = org_client.get_paginator('list_targets_for_policy')
                for page in paginator.paginate(PolicyId=policy['Id']):
                    targets.extend(page['Targets'])
                
                policy_details.append({
                    'id': policy['Id'],
                    'name': policy['Name'],
                    'description': policy.get('Description', ''),
                    'content': policy_detail['Policy']['Content'],
                    'targets': targets
                })
            
            results['policies_analyzed'][policy_type] = policy_details
            
            # Analyze policy coverage
            covered_accounts = set()
            covered_ous = set()
            
            for policy in policy_details:
                for target in policy['targets']:
                    if target['Type'] == 'ACCOUNT':
                        covered_accounts.add(target['TargetId'])
                    elif target['Type'] == 'ORGANIZATIONAL_UNIT':
                        covered_ous.add(target['TargetId'])
            
            # Calculate coverage
            total_accounts = len(accounts)
            covered_account_count = len(covered_accounts)
            coverage_percentage = (covered_account_count / total_accounts * 100) if total_accounts > 0 else 0
            
            results['policy_coverage'][policy_type] = {
                'total_accounts': total_accounts,
                'covered_accounts': covered_account_count,
                'coverage_percentage': coverage_percentage,
                'uncovered_accounts': [
                    {
                        'id': account['Id'],
                        'name': account['Name'],
                        'email': account['Email']
                    }
                    for account in accounts if account['Id'] not in covered_accounts
                ]
            }
            
            # Special analysis for SCPs
            if policy_type == 'SERVICE_CONTROL_POLICY':
                # Look for potential conflicts or overly restrictive policies
                for i, policy1 in enumerate(policy_details):
                    policy1_content = json.loads(policy1['content'])
                    
                    # Check for deny statements that might conflict with other policies
                    if 'Statement' in policy1_content:
                        for statement in policy1_content['Statement']:
                            if statement.get('Effect') == 'Deny' and 'Action' in statement:
                                # Check other policies for potential conflicts
                                for j, policy2 in enumerate(policy_details):
                                    if i != j:  # Don't compare with self
                                        policy2_content = json.loads(policy2['content'])
                                        
                                        if 'Statement' in policy2_content:
                                            for statement2 in policy2_content['Statement']:
                                                if statement2.get('Effect') == 'Allow' and 'Action' in statement2:
                                                    # Check for overlapping actions
                                                    actions1 = statement['Action'] if isinstance(statement['Action'], list) else [statement['Action']]
                                                    actions2 = statement2['Action'] if isinstance(statement2['Action'], list) else [statement2['Action']]
                                                    
                                                    # Check for wildcards and overlaps
                                                    for action1 in actions1:
                                                        for action2 in actions2:
                                                            if action1 == action2 or action1 == '*' or action2 == '*' or \
                                                               (action1.endswith('*') and action2.startswith(action1[:-1])) or \
                                                               (action2.endswith('*') and action1.startswith(action2[:-1])):
                                                                results['potential_conflicts'].append({
                                                                    'policy_type': policy_type,
                                                                    'policy1': {
                                                                        'id': policy1['id'],
                                                                        'name': policy1['name'],
                                                                        'effect': 'Deny',
                                                                        'action': action1
                                                                    },
                                                                    'policy2': {
                                                                        'id': policy2['id'],
                                                                        'name': policy2['name'],
                                                                        'effect': 'Allow',
                                                                        'action': action2
                                                                    }
                                                                })
        except Exception as e:
            results['policies_analyzed'][policy_type] = {
                'error': str(e)
            }
    
    # Generate recommendations
    if results['policy_coverage'].get('SERVICE_CONTROL_POLICY', {}).get('coverage_percentage', 100) < 100:
        results['recommendations'].append({
            'type': 'coverage',
            'severity': 'high',
            'message': 'Some accounts are not covered by any Service Control Policies',
            'details': f"{results['policy_coverage']['SERVICE_CONTROL_POLICY']['uncovered_accounts']} accounts are not covered"
        })
    
    if len(results['potential_conflicts']) > 0:
        results['recommendations'].append({
            'type': 'conflict',
            'severity': 'medium',
            'message': 'Potential conflicts detected between policies',
            'details': f"Found {len(results['potential_conflicts'])} potential conflicts between Allow and Deny statements"
        })
    
    # Save report to S3 if configured
    if report_bucket and s3_client:
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            report_key = f"{report_prefix}{timestamp}-policy-analysis.json"
            
            s3_client.put_object(
                Bucket=report_bucket,
                Key=report_key,
                Body=json.dumps(results, indent=2),
                ContentType='application/json'
            )
            
            results['report_location'] = f"s3://{report_bucket}/{report_key}"
        except Exception as e:
            results['report_error'] = str(e)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'AWS Organizations policy analysis completed',
            'results': results
        })
    }