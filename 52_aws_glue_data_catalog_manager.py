import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to manage AWS Glue Data Catalog (databases, tables, partitions).
    
    This function can perform actions based on the 'event' payload:
    - Create/Update/Delete Glue Database
    - Create/Update/Delete Glue Table
    - Add/Delete Glue Partitions
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications of success/failure.

    Event Structure (example for creating a table):
    {
        "action": "CREATE_TABLE",
        "catalog_id": "123456789012", // Optional, defaults to AWS account ID
        "database_name": "my_database",
        "table_input": {
            "Name": "my_new_table",
            "Description": "A sample table",
            "StorageDescriptor": {
                "Columns": [{"Name": "col1", "Type": "string"}],
                "Location": "s3://my-bucket/my-table-data/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                }
            },
            "PartitionKeys": [{"Name": "year", "Type": "int"}]
        }
    }
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    # Initialize AWS clients
    glue_client = boto3.client('glue', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'action_performed': event.get('action'),
        'status': 'Pending',
        'details': {},
        'error': None
    }

    action = event.get('action')
    catalog_id = event.get('catalog_id') # If None, Glue uses the caller's account ID
    database_name = event.get('database_name')

    try:
        if not action:
            raise ValueError("Missing 'action' in event payload.")

        if action == "CREATE_DATABASE":
            if not database_name:
                raise ValueError("Missing 'database_name' for CREATE_DATABASE action.")
            db_input = event.get('database_input', {'Name': database_name})
            params = {'DatabaseInput': db_input}
            if catalog_id: params['CatalogId'] = catalog_id
            glue_client.create_database(**params)
            results['details'] = f"Database '{database_name}' created successfully."
            results['status'] = 'Success'

        elif action == "CREATE_TABLE":
            if not database_name or not event.get('table_input'):
                raise ValueError("Missing 'database_name' or 'table_input' for CREATE_TABLE action.")
            table_input = event['table_input']
            params = {'DatabaseName': database_name, 'TableInput': table_input}
            if catalog_id: params['CatalogId'] = catalog_id
            glue_client.create_table(**params)
            results['details'] = f"Table '{table_input['Name']}' created in database '{database_name}'."
            results['status'] = 'Success'
        
        elif action == "UPDATE_TABLE":
            if not database_name or not event.get('table_input'):
                raise ValueError("Missing 'database_name' or 'table_input' for UPDATE_TABLE action.")
            table_input = event['table_input']
            params = {'DatabaseName': database_name, 'TableInput': table_input}
            if catalog_id: params['CatalogId'] = catalog_id
            glue_client.update_table(**params)
            results['details'] = f"Table '{table_input['Name']}' in database '{database_name}' updated."
            results['status'] = 'Success'

        elif action == "DELETE_TABLE":
            if not database_name or not event.get('table_name'):
                raise ValueError("Missing 'database_name' or 'table_name' for DELETE_TABLE action.")
            table_name = event['table_name']
            params = {'DatabaseName': database_name, 'Name': table_name}
            if catalog_id: params['CatalogId'] = catalog_id
            glue_client.delete_table(**params)
            results['details'] = f"Table '{table_name}' deleted from database '{database_name}'."
            results['status'] = 'Success'

        # Add more actions: DELETE_DATABASE, CREATE_PARTITION, BATCH_CREATE_PARTITION, etc.
        else:
            raise ValueError(f"Unsupported action: {action}")

        if sns and results['status'] == 'Success':
            sns.publish(TopicArn=sns_topic_arn, Message=json.dumps(results, indent=2), Subject=f"Glue Catalog Manager: {action} Success")

    except Exception as e:
        results['error'] = str(e)
        results['status'] = 'Error'
        if sns:
            sns.publish(TopicArn=sns_topic_arn, Message=json.dumps(results, indent=2), Subject=f"Glue Catalog Manager: {action} Error")
        return {
            'statusCode': 500,
            'body': json.dumps(results)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }

if __name__ == '__main__':
    # Example usage for local testing
    os.environ['SNS_TOPIC_ARN'] = 'arn:aws:sns:us-east-1:123456789012:MySNSTopic' # Replace
    
    example_create_table_event = {
        "action": "CREATE_TABLE",
        # "catalog_id": "123456789012", 
        "database_name": "my_test_db", # Ensure this DB exists or create it first
        "table_input": {
            "Name": "my_lambda_managed_table",
            "Description": "Table created by Lambda",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"}
                ],
                "Location": "s3://your-bucket-name/glue-test-data/my_lambda_managed_table/", # Replace with your S3 path
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {"field.delim": ","}
                }
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "csv"} # Example parameter
        }
    }
    # First, ensure 'my_test_db' exists. You can use an event for CREATE_DATABASE:
    # example_create_db_event = {
    #     "action": "CREATE_DATABASE",
    #     "database_name": "my_test_db"
    # }
    # print("Creating DB (if not exists):", lambda_handler(example_create_db_event, {}))
    
    # Then create the table:
    # print("Creating Table:", lambda_handler(example_create_table_event, {}))

    # Example for deleting the table:
    # example_delete_table_event = {
    #     "action": "DELETE_TABLE",
    #     "database_name": "my_test_db",
    #     "table_name": "my_lambda_managed_table"
    # }
    # print("Deleting Table:", lambda_handler(example_delete_table_event, {}))
    print("Please uncomment and configure example events in __main__ to test.")