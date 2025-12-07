import json
import os
import logging
import requests
import boto3

# Configure logging for CloudWatch
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Initialize SQS client
sqs = boto3.client("sqs")


def get_environment_variable(key, required=True):
    value = os.environ.get(key)
    if required and not value:
        logger.error(f"{key} environment variable not set")
        raise EnvironmentError(f"{key} environment variable not set")
    return value


def make_nuxeo_request(url, headers, payload, auth):
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            auth=auth,
            timeout=5
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise


def process_documents(documents, queue_url):
    for document in documents:
        if document.get("properties", {}).get("picture:views", []):
            for view in document.get("properties", {}).get("picture:views", []):
                if view.get("title") == "FullHD":
                    digest = view.get("content", {}).get("digest", "unknown")
                    break
        else:
            digest = document.get("properties", {}).get("file:content", {}).get("digest", "unknown")
        
        document_uuid = document.get("uid")
        if digest == "unknown":
            logger.warning(f"Digest not found for document {document_uuid}")
            continue

        binary_key = f"nike-binary/{digest}"
        sqs_payload = {
            "Records": [
                {
                    "s3": {
                        "bucket": {
                            "name": "******",
                            "arn": "******"
                        },
                        "object": {
                            "key": binary_key
                        },
                        "documentUUID": {
                            "uid": document_uuid
                        }
                    }
                }
            ]
        }

        try:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(sqs_payload)
            )
            logger.info(f"Message sent to SQS for document {document_uuid} {binary_key}")
        except Exception as e:
            logger.error(f"Failed to send message for document {document_uuid} {binary_key}: {str(e)}")


def lambda_handler(event, context):
    try:
        # Fetch environment variables
        url = get_environment_variable("Nuxeo_Endpoint")
        username = get_environment_variable("Nuxeo_UserName")
        password = get_environment_variable("Nuxeo_Password")
        queue_url = get_environment_variable("SQS_QUEUE_URL")

        # Prepare headers and payload
        headers = {
            "Nuxeo-Transaction-Timeout": "3",
            "X-NXproperties": "*",
            "X-NXRepository": "default",
            "X-NXVoidOperation": "false",
            "content-type": "application/json"
        }
        payload = {
            "params": {},
            "input": event["collectionId"],
            "context": {}
        }

        # Make Nuxeo API request
        response_data = make_nuxeo_request(url, headers, payload, (username, password))
        documents = response_data.get("entries", [])

        if not documents:
            logger.info("No documents found in the collection.")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "No documents found in the collection."})
            }

        # Process documents and send messages to SQS
        process_documents(documents, queue_url)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Messages sent to SQS successfully."})
        }

    except EnvironmentError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "An unexpected error occurred."})
        }