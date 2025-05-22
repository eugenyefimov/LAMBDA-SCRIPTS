import boto3
import json
import os
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """
    AWS Lambda function to optimize AWS Athena queries.
    
    This function analyzes Athena query patterns and provides optimization recommendations:
    - Identifies slow-running queries
    - Recommends partitioning strategies
    - Suggests query rewrites for better performance
    - Monitors query costs and data scanned
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - WORKGROUP: Athena workgroup to analyze (default: primary)
    - LOOKBACK_DAYS: Days of query history to analyze (default: 30)
    - SLOW_QUERY_THRESHOLD_MS: Threshold for slow queries in milliseconds (default: 60000)
    - S3_REPORT_BUCKET: Optional S3 bucket for storing reports
    - SNS_TOPIC_ARN: Optional SNS topic ARN for notifications
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    workgroup = os.environ.get('WORKGROUP', 'primary')
    lookback_days = int(os.environ.get('LOOKBACK_DAYS', 30))
    slow_query_threshold = int(os.environ.get('SLOW_QUERY_THRESHOLD_MS', 60000))
    s3_report_bucket = os.environ.get('S3_REPORT_BUCKET', '')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    
    # Initialize AWS clients
    athena = boto3.client('athena', region_name=region)
    s3 = boto3.client('s3', region_name=region) if s3_report_bucket else None
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    
    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=lookback_days)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'workgroup': workgroup,
        'queries_analyzed': 0,
        'slow_queries': [],
        'tables_to_partition': [],
        'optimization_recommendations': []
    }
    
    # Get query execution history
    query_executions = []
    try:
        paginator = athena.get_paginator('list_query_executions')
        for page in paginator.paginate(WorkGroup=workgroup):
            query_execution_ids = page.get('QueryExecutionIds', [])
            
            # Get details for each query execution
            for i in range(0, len(query_execution_ids), 50):  # Process in batches of 50
                batch = query_execution_ids[i:i+50]
                response = athena.batch_get_query_execution(QueryExecutionIds=batch)
                
                for query_execution in response['QueryExecutions']:
                    # Check if query is within our time range
                    submission_time = query_execution.get('Status', {}).get('SubmissionDateTime')
                    if submission_time and submission_time >= start_time:
                        query_executions.append(query_execution)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error retrieving Athena query executions',
                'error': str(e)
            })
        }
    
    # Analyze query executions
    table_stats = {}
    database_stats = {}
    
    for execution in query_executions:
        results['queries_analyzed'] += 1
        
        # Extract query details
        query_id = execution['QueryExecutionId']
        query_sql = execution.get('Query', '')
        status = execution.get('Status', {})
        statistics = execution.get('Statistics', {})
        
        # Extract database and table information
        database = execution.get('QueryExecutionContext', {}).get('Database', '')
        
        # Extract tables from query (simple approach)
        tables = []
        sql_lower = query_sql.lower()
        if 'from ' in sql_lower:
            from_parts = sql_lower.split('from ')[1].split()
            if from_parts:
                table = from_parts[0].strip().rstrip(';')
                if '.' in table:
                    tables.append(table)
                elif database:
                    tables.append(f"{database}.{table}")
        
        # Track database and table statistics
        if database:
            if database not in database_stats:
                database_stats[database] = {
                    'query_count': 0,
                    'data_scanned_bytes': 0,
                    'execution_time_ms': 0
                }
            database_stats[database]['query_count'] += 1
            
            if 'data_scanned_in_bytes' in statistics:
                database_stats[database]['data_scanned_bytes'] += statistics['data_scanned_in_bytes']
            
            if 'TotalExecutionTimeInMillis' in statistics:
                database_stats[database]['execution_time_ms'] += statistics['TotalExecutionTimeInMillis']
        
        # Track table statistics
        for table in tables:
            if table not in table_stats:
                table_stats[table] = {
                    'query_count': 0,
                    'data_scanned_bytes': 0,
                    'execution_time_ms': 0,
                    'has_where_clause': 0
                }
            table_stats[table]['query_count'] += 1
            
            if 'data_scanned_in_bytes' in statistics:
                table_stats[table]['data_scanned_bytes'] += statistics['data_scanned_in_bytes']
            
            if 'TotalExecutionTimeInMillis' in statistics:
                table_stats[table]['execution_time_ms'] += statistics['TotalExecutionTimeInMillis']
            
            # Check for WHERE clause
            if 'where ' in sql_lower:
                table_stats[table]['has_where_clause'] += 1
        
        # Check for slow queries
        execution_time = statistics.get('TotalExecutionTimeInMillis', 0)
        if execution_time > slow_query_threshold:
            results['slow_queries'].append({
                'query_id': query_id,
                'execution_time_ms': execution_time,
                'data_scanned_bytes': statistics.get('DataScannedInBytes', 0),
                'query_sql': query_sql[:1000] + ('...' if len(query_sql) > 1000 else ''),
                'submission_time': status.get('SubmissionDateTime', '').isoformat() if status.get('SubmissionDateTime') else None,
                'completion_time': status.get('CompletionDateTime', '').isoformat() if status.get('CompletionDateTime') else None
            })
    
    # Generate recommendations for partitioning
    for table, stats in table_stats.items():
        if stats['query_count'] >= 5 and stats['has_where_clause'] / stats['query_count'] >= 0.7:
            # Table is queried frequently with WHERE clauses - good candidate for partitioning
            results['tables_to_partition'].append({
                'table': table,
                'query_count': stats['query_count'],
                'data_scanned_bytes': stats['data_scanned_bytes'],
                'avg_execution_time_ms': stats['execution_time_ms'] / stats['query_count'] if stats['query_count'] > 0 else 0,
                'where_clause_percentage': (stats['has_where_clause'] / stats['query_count']) * 100 if stats['query_count'] > 0 else 0
            })
    
    # Generate general optimization recommendations
    if results['slow_queries']:
        results['optimization_recommendations'].append({
            'type': 'query_optimization',
            'description': 'Consider optimizing slow queries by adding appropriate WHERE clauses and using partitioned tables',
            'impact': 'high',
            'affected_queries': len(results['slow_queries'])
        })
    
    if results['tables_to_partition']:
        results['optimization_recommendations'].append({
            'type': 'partitioning',
            'description': 'Consider partitioning frequently queried tables to reduce data scanned and improve performance',
            'impact': 'high',
            'affected_tables': len(results['tables_to_partition'])
        })
    
    # Check for compression opportunities
    high_data_scan_tables = [t for t in results['tables_to_partition'] if t['data_scanned_bytes'] > 1073741824]  # > 1GB
    if high_data_scan_tables:
        results['optimization_recommendations'].append({
            'type': 'compression',
            'description': 'Consider using compressed formats (Parquet, ORC) for tables with high data scan volumes',
            'impact': 'medium',
            'affected_tables': len(high_data_scan_tables)
        })
    
    # Send notification if SNS topic is configured
    if sns and sns_topic_arn and (results['slow_queries'] or results['tables_to_partition']):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"AWS Athena Query Optimization Report - {datetime.now().strftime('%Y-%m-%d')}",
                Message=json.dumps({
                    'slow_queries_count': len(results['slow_queries']),
                    'tables_to_partition': results['tables_to_partition'],
                    'optimization_recommendations': results['optimization_recommendations']
                }, indent=2)
            )
        except Exception as e:
            print(f"Error sending SNS notification: {str(e)}")
    
    # Store report in S3 if bucket is configured
    if s3 and s3_report_bucket:
        try:
            report_key = f"{workgroup}/athena-optimization-report-{datetime.now().strftime('%Y-%m-%d')}.json"
            s3.put_object(
                Bucket=s3_report_bucket,
                Key=report_key,
                Body=json.dumps(results, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Error storing report in S3: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }