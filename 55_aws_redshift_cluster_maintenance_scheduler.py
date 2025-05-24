import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to manage AWS Redshift cluster maintenance windows or perform actions.
    
    This function can:
    - Modify maintenance windows for specified Redshift clusters.
    - Defer maintenance if needed (within allowed limits).
    - Potentially trigger other maintenance-related tasks (e.g., pre/post scripts).
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - REDSHIFT_CLUSTER_IDENTIFIERS: Comma-separated list of Redshift Cluster Identifiers.
    - ACTION: 'SET_WINDOW', 'DEFER_MAINTENANCE' (more can be added).
    - PREFERRED_MAINTENANCE_WINDOW: For 'SET_WINDOW', e.g., "sat:03:00-sat:03:30" (UTC).
    - DEFER_MAINTENANCE_UNTIL: For 'DEFER_MAINTENANCE', ISO 8601 timestamp for new deferral end time.
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications.
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    cluster_ids_str = os.environ.get('REDSHIFT_CLUSTER_IDENTIFIERS')
    action = os.environ.get('ACTION', '').upper()
    preferred_window = os.environ.get('PREFERRED_MAINTENANCE_WINDOW') # e.g., "ddd:hh:mm-ddd:hh:mm"
    defer_until_str = os.environ.get('DEFER_MAINTENANCE_UNTIL')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    if not cluster_ids_str:
        return {'statusCode': 400, 'body': json.dumps('Missing env var: REDSHIFT_CLUSTER_IDENTIFIERS')}
    if not action:
        return {'statusCode': 400, 'body': json.dumps('Missing env var: ACTION')}

    cluster_ids = [cid.strip() for cid in cluster_ids_str.split(',')]

    # Initialize AWS clients
    redshift_client = boto3.client('redshift', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'action_taken': action,
        'successful_clusters': [],
        'failed_clusters': [],
        'errors': []
    }

    for cluster_id in cluster_ids:
        try:
            if action == 'SET_WINDOW':
                if not preferred_window:
                    raise ValueError("PREFERRED_MAINTENANCE_WINDOW is required for SET_WINDOW action.")
                redshift_client.modify_cluster_maintenance(
                    ClusterIdentifier=cluster_id,
                    PreferredMaintenanceWindow=preferred_window
                )
                results['successful_clusters'].append({'ClusterId': cluster_id, 'Detail': f"Maintenance window set to {preferred_window}"})
                if sns:
                    sns.publish(TopicArn=sns_topic_arn, Message=f"Redshift cluster {cluster_id} maintenance window set to {preferred_window}.", Subject=f"Redshift Maintenance Update: {cluster_id}")
            
            elif action == 'DEFER_MAINTENANCE':
                if not defer_until_str:
                    raise ValueError("DEFER_MAINTENANCE_UNTIL is required for DEFER_MAINTENANCE action.")
                
                # DeferMaintenanceIdentifier is usually 'REDSHIFT_UPCOMING_MAINTENANCE'
                # Need to describe pending maintenance first to get the identifier and duration.
                # This is a simplified example. A real one would call describe_cluster_pending_maintenance.
                # For now, let's assume we have a known deferrable maintenance.
                # The API requires DeferMaintenanceStartTime and DeferMaintenanceEndTime or DeferMaintenanceDuration.
                # This example is too basic for a full deferral implementation without more inputs or logic.
                # Let's simulate a deferral request if the API allowed a simple 'defer until' directly.
                # The actual API `defer_cluster_maintenance` needs more specific parameters.
                # For now, this part is a placeholder for a more complex logic.
                results['failed_clusters'].append({'ClusterId': cluster_id, 'Error': 'DEFER_MAINTENANCE action is complex and requires describing pending maintenance first. This is a placeholder.'})
                # Example of what might be needed:
                # pending_maint = redshift_client.describe_pending_maintenance_actions(ResourceIdentifier=cluster_id)
                # if pending_maint['PendingMaintenanceActions']:
                #    action_details = pending_maint['PendingMaintenanceActions'][0]
                #    defer_identifier = action_details['PendingMaintenanceActionName'] # This might not be the right ID
                #    # ... calculate new start/end or duration based on defer_until_str
                #    redshift_client.defer_cluster_maintenance(...)
                # else: # No pending maintenance to defer

            else:
                raise ValueError(f"Unsupported ACTION: {action}")

        except Exception as e:
            error_msg = f"Error performing '{action}' on Redshift cluster {cluster_id}: {str(e)}"
            results['errors'].append(error_msg)
            results['failed_clusters'].append({'ClusterId': cluster_id, 'Error': str(e)})
            if sns:
                sns.publish(TopicArn=sns_topic_arn, Message=error_msg, Subject=f"Redshift Maintenance Error: {cluster_id}")

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['REDSHIFT_CLUSTER_IDENTIFIERS'] = 'my-redshift-cluster' # Replace
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace

    # Example 1: Set Maintenance Window
    os.environ['ACTION'] = 'SET_WINDOW'
    os.environ['PREFERRED_MAINTENANCE_WINDOW'] = 'sun:05:00-sun:05:30' # Example: Sunday 5:00-5:30 AM UTC
    # print("Setting Maintenance Window:", lambda_handler({}, {}))

    # Example 2: Defer Maintenance (Conceptual - API needs more details)
    # os.environ['ACTION'] = 'DEFER_MAINTENANCE'
    # os.environ['DEFER_MAINTENANCE_UNTIL'] = (datetime.utcnow() + timedelta(days=7)).isoformat()
    # print("Deferring Maintenance (Conceptual):", lambda_handler({}, {}))
    print("Please uncomment and configure example actions in __main__ to test.")