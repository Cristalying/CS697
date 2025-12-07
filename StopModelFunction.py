import json
import boto3
import os
import requests
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_rekognition_client():
    """Initialize and return the Rekognition client."""
    return boto3.client('rekognition')

def get_environment_variable(key, required=True):
    """Fetch an environment variable, raising an error if required and not set."""
    value = os.environ.get(key)
    if required and not value:
        logger.error(f"{key} environment variable not set")
        raise EnvironmentError(f"{key} environment variable not set")
    return value

def get_environment_variables():
    """Fetch all required environment variables."""
    try:
        return {
            "project_version_arn": get_environment_variable('rekog_model_project_version_arn'),
            "project_arn": get_environment_variable('rekog_model_project_arn'),
            "nuxeo_endpoint": get_environment_variable("Nuxeo_Endpoint"),
            "nuxeo_username": get_environment_variable("Nuxeo_UserName"),
            "nuxeo_password": get_environment_variable("Nuxeo_Password")
        }
    except EnvironmentError as e:
        logger.error(f"Environment variable error: {e}")
        raise

def check_model_running_status(rekog_client, project_arn, project_version_name):
    """Check the running status of the Rekognition model."""
    try:
        response = rekog_client.describe_project_versions(
            ProjectArn=project_arn,
            VersionNames=[project_version_name]
        )
        return response['ProjectVersionDescriptions'][0]['Status']
    except Exception as e:
        logger.error(f"Error checking model status: {e}")
        return None

def stop_model_if_running(rekog_client, project_version_arn, running_status, running_states):
    """Stop the Rekognition model if it is in a running state."""
    if running_status in running_states:
        try:
            rekog_client.stop_project_version(ProjectVersionArn=project_version_arn)
            logger.info(f"Model stopped successfully. Status: {running_status}")
        except Exception as e:
            logger.error(f"Error stopping model: {e}")
    else:
        logger.info(f"Model is not running. Status: {running_status}")

def send_nuxeo_request(event, env_vars):
    """Send a request to the Nuxeo API."""
    user_email = event.get('userEmail')
    if not user_email:
        logger.warning("User email is empty. Skipping Nuxeo request.")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "User email is required to send Nuxeo request."})
        }

    headers = {
        "Nuxeo-Transaction-Timeout": "3",
        "X-NXproperties": "*",
        "X-NXRepository": "default",
        "X-NXVoidOperation": "false",
        "content-type": "application/json"
    }

    payload = {
        "params": {"from": "no-reply@maildrop.cc", "to": user_email, "HTML": True},
        "input": event.get('collectionId'),
        "context": {}
    }

    try:
        response = requests.post(
            env_vars["nuxeo_endpoint"],
            headers=headers,
            json=payload,
            auth=(env_vars["nuxeo_username"], env_vars["nuxeo_password"]),
            timeout=5
        )
        logger.info(f"Nuxeo response status: {response.status_code}")
        response.raise_for_status()

        if 200 <= response.status_code < 300:
            return {"statusCode": response.status_code}
        else:
            logger.error("Unexpected response status code from Nuxeo API")
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Unexpected response status code from Nuxeo API"})
            }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending Nuxeo request: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to send Nuxeo request"})
        }

def lambda_handler(event, context):
    """Main Lambda function handler."""
    try:
        env_vars = get_environment_variables()
        rekog_client = get_rekognition_client()
        running_states = ['STARTING', 'RUNNING']
        project_version_name = env_vars["project_version_arn"].split("/")[3]

        running_status = check_model_running_status(
            rekog_client, env_vars["project_arn"], project_version_name
        )
        if running_status:
            stop_model_if_running(rekog_client, env_vars["project_version_arn"], running_status, running_states)

        return send_nuxeo_request(event, env_vars)
    except EnvironmentError as e:
        logger.error(f"Environment error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "An unexpected error occurred."})
        }