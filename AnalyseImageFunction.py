import json
import os
import boto3
import logging
import requests
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    # Nuxeo API endpoint and credentials
    nuxeo_url = os.environ.get("Nuxeo_Endpoint")
    nuxeo_user = os.environ.get("Nuxeo_User")
    nuxeo_password = os.environ.get("Nuxeo_Password")  # Ensure this is set in environment variables
    dlq_url = os.environ.get("DLQ_URL")  # Dead Letter Queue URL

    if not dlq_url:
        logger.error("DLQ_URL environment variable not set")
        return {'status': '500', 'error': 'DLQ_URL not configured'}

    # Create boto3 clients
    s3_client = boto3.client('s3')
    rekognition_client = boto3.client('rekognition')
    sqs_client = boto3.client('sqs')
    model_arn = os.environ['rekognition_model_project_version_arn']

    processed_messages = []
    failed_messages = []

    for msg in event["Records"]:
        try:
            msg_payload = json.loads(msg["body"])
            logger.info(f"Message payload: {msg_payload}")

            if "Records" not in msg_payload:
                logger.error(f"Invalid message, sending to DLQ: {msg_payload}")
                failed_messages.append(msg)
                continue

            # Extract S3 and document details
            record = msg_payload["Records"][0]
            bucket = record["s3"]["bucket"]["name"]
            image = record["s3"]["object"]["key"].replace("+", " ")
            doc_uid = record["s3"]["documentUUID"]["uid"]

            if not all([bucket, image, doc_uid]):
                logger.error("Missing required fields in message, sending to DLQ")
                failed_messages.append(msg)
                continue

            logger.info(f"Processing: bucket={bucket}, image={image}, docUid={doc_uid}")

            # Call Rekognition to detect custom labels
            response = rekognition_client.detect_custom_labels(
                ProjectVersionArn=model_arn,
                Image={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': image
                    }
                }
            )

            # Get the custom labels
            labels = response['CustomLabels']
            logger.info(f"Detected labels: {labels}")

            # Prepare labels for Nuxeo
            label_names = [label['Name'] for label in labels]
            labels_value = ",".join(label_names) if label_names else "none"

            # Prepare Nuxeo API request
            nuxeo_payload = {
                "params": {
                    "xpath": "assetRecognition:landMark",
                    "save": "true",
                    "value": labels_value
                },
                "input": doc_uid,
                "context": {}
            }

            headers = {
                'Nuxeo-Transaction-Timeout': '3',
                'X-NXproperties': '*',
                'X-NXRepository': 'default',
                'X-NXVoidOperation': 'false',
                'content-type': 'application/json'
            }

            # Make Nuxeo API call to set property
            nuxeo_response = requests.post(
                nuxeo_url,
                json=nuxeo_payload,
                headers=headers,
                auth=(nuxeo_user, nuxeo_password),
                timeout=5
            )
            nuxeo_response.raise_for_status()
            logger.info(f"Nuxeo API response: {nuxeo_response.status_code}")

            processed_messages.append({
                'docUid': doc_uid,
                'labels': label_names,
                'nuxeo_status': nuxeo_response.status_code
            })

        except ClientError as e:
            logger.error(f"Rekognition error: {e}, sending to DLQ")
            failed_messages.append(msg)
            continue
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}, sending to DLQ")
            failed_messages.append(msg)
            continue
        except requests.RequestException as e:
            logger.error(f"Nuxeo API error: {e}, sending to DLQ")
            failed_messages.append(msg)
            continue
        except Exception as e:
            logger.error(f"Unexpected error: {e}, sending to DLQ")
            failed_messages.append(msg)
            continue

    # Send failed messages to DLQ
    for failed_msg in failed_messages:
        try:
            sqs_client.send_message(
                QueueUrl=dlq_url,
                MessageBody=failed_msg['body']
            )
            logger.info(f"Sent message to DLQ: {failed_msg['messageId']}")
        except ClientError as e:
            logger.error(f"Failed to send message to DLQ: {e}")

    return {
        'status': '200',
        'processed': len(processed_messages),
        'failed': len(failed_messages),
        'results': processed_messages
    }