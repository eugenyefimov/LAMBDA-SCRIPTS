import boto3
import json
import os
from datetime import datetime, timedelta
import time

def lambda_handler(event, context):
    """
    AWS Lambda function to monitor ACM certificate expiration.
    
    This function checks AWS Certificate Manager certificates and:
    - Identifies certificates nearing expiration
    - Sends notifications for certificates that need renewal
    - Optionally triggers automatic renewal for eligible certificates
    - Generates expiration reports
    
    Environment Variables:
    - REGION: AWS region to operate in (default: us-east-1)
    - WARNING_DAYS: Days before expiration to start warning (default: 45)
    - CRITICAL_DAYS: Days before expiration for critical alerts (default: 15)
    - SNS_TOPIC_ARN: SNS topic ARN for notifications
    - AUTO_RENEW: Whether to attempt auto-renewal (default: false)
    - REPORT_S3_BUCKET: Optional S3 bucket for storing reports
    """
    # Get configuration from environment variables
    region = os.environ.get('REGION', 'us-east-1')
    warning_days = int(os.environ.get('WARNING_DAYS', 45))
    critical_days = int(os.environ.get('CRITICAL_DAYS', 15))
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', '')
    auto_renew = os.environ.get('AUTO_RENEW', 'false').lower() == 'true'
    report_bucket = os.environ.get('REPORT_S3_BUCKET', '')
    
    # Initialize AWS clients
    acm = boto3.client('acm', region_name=region)
    sns = boto3.client('sns', region_name=region) if sns_topic_arn else None
    s3 = boto3.client('s3', region_name=region) if report_bucket else None
    
    # Get current time
    now = datetime.now()
    
    # Get all certificates
    certificates = []
    paginator = acm.get_paginator('list_certificates')
    for page in paginator.paginate():
        certificates.extend(page['CertificateSummaryList'])
    
    # Check each certificate
    warning_certs = []
    critical_certs = []
    expired_certs = []
    
    for cert_summary in certificates:
        cert_arn = cert_summary['CertificateArn']
        
        # Get certificate details
        cert = acm.describe_certificate(CertificateArn=cert_arn)
        cert_detail = cert['Certificate']
        
        # Skip certificates that don't have an expiration date
        if 'NotAfter' not in cert_detail:
            continue
        
        # Calculate days until expiration
        expiry_date = cert_detail['NotAfter']
        days_until_expiry = (expiry_date - now).days
        
        # Check if certificate is expired or nearing expiration
        if days_until_expiry <= 0:
            expired_certs.append({
                'arn': cert_arn,
                'domain': cert_detail['DomainName'],
                'expiry_date': expiry_date.isoformat(),
                'days_until_expiry': days_until_expiry
            })
        elif days_until_expiry <= critical_days:
            critical_certs.append({
                'arn': cert_arn,
                'domain': cert_detail['DomainName'],
                'expiry_date': expiry_date.isoformat(),
                'days_until_expiry': days_until_expiry
            })
        elif days_until_expiry <= warning_days:
            warning_certs.append({
                'arn': cert_arn,
                'domain': cert_detail['DomainName'],
                'expiry_date': expiry_date.isoformat(),
                'days_until_expiry': days_until_expiry
            })
        
        # Attempt auto-renewal if enabled and certificate is eligible
        if auto_renew and days_until_expiry <= critical_days:
            # Only attempt renewal for eligible certificates (issued by ACM)
            if cert_detail.get('Type') == 'AMAZON_ISSUED':
                try:
                    acm.renew_certificate(CertificateArn=cert_arn)
                except Exception as e:
                    print(f"Error renewing certificate {cert_arn}: {str(e)}")
    
    # Prepare notification message
    if sns and sns_topic_arn and (warning_certs or critical_certs or expired_certs):
        message = {
            'timestamp': now.isoformat(),
            'region': region,
            'summary': {
                'total_certificates': len(certificates),
                'warning_certificates': len(warning_certs),
                'critical_certificates': len(critical_certs),
                'expired_certificates': len(expired_certs)
            },
            'expired_certificates': expired_certs,
            'critical_certificates': critical_certs,
            'warning_certificates': warning_certs
        }
        
        # Send notification
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"ACM Certificate Expiration Report - {len(critical_certs)} Critical, {len(expired_certs)} Expired",
            Message=json.dumps(message, indent=2)
        )
    
    # Save report to S3 if configured
    if s3 and report_bucket:
        report = {
            'timestamp': now.isoformat(),
            'region': region,
            'certificates': {
                'total': len(certificates),
                'warning': warning_certs,
                'critical': critical_certs,
                'expired': expired_certs
            }
        }
        
        # Upload report to S3
        report_key = f"acm-reports/{now.strftime('%Y-%m-%d')}-certificate-report.json"
        s3.put_object(
            Bucket=report_bucket,
            Key=report_key,
            Body=json.dumps(report, indent=2),
            ContentType='application/json'
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'ACM certificate expiration check completed',
            'summary': {
                'total_certificates': len(certificates),
                'warning_certificates': len(warning_certs),
                'critical_certificates': len(critical_certs),
                'expired_certificates': len(expired_certs)
            }
        })
    }