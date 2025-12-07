import boto3
import os
import logging

# Set up logger for AWS Lambda (CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_rekognition_client():
    logger.info("Initializing Rekognition client.")
    return boto3.client('rekognition')

def get_environment_variables():
    logger.info("Fetching environment variables.")
    return {
        "project_version_arn": os.environ['rekog_model_project_version_arn'],
        "project_arn": os.environ['rekog_model_project_arn']
    }

def get_project_version_name(project_version_arn):
    logger.info(f"Extracting project version name from ARN: {project_version_arn}")
    return project_version_arn.split("/")[3]

def describe_project_version(rekog_client, project_arn, version_name):
    logger.info(f"Describing project version: {version_name} for project ARN: {project_arn}")
    try:
        response = rekog_client.describe_project_versions(
            ProjectArn=project_arn,
            VersionNames=[version_name]
        )
        status = response['ProjectVersionDescriptions'][0]['Status']
        logger.info(f"Project version status: {status}")
        return status
    except Exception as e:
        logger.error(f"Error describing project version: {e}")
        raise

def start_project_version(rekog_client, project_version_arn):
    logger.info(f"Starting project version with ARN: {project_version_arn}")
    try:
        response = rekog_client.start_project_version(
            ProjectVersionArn=project_version_arn,
            MinInferenceUnits=1
        )
        logger.info(f"Start project version response: {response}")
        return response
    except Exception as e:
        logger.error(f"Error starting project version: {e}")
        raise

def lambda_handler(event, context):
    logger.info("Lambda handler invoked.")
    rekog_client = get_rekognition_client()
    env_vars = get_environment_variables()
    project_version_name = get_project_version_name(env_vars["project_version_arn"])

    logger.info("Checking project version status.")
    running_status = describe_project_version(
        rekog_client, env_vars["project_arn"], project_version_name
    )

    if running_status in ['RUNNING', 'STARTING']:
        logger.info(f"Model is already in status: {running_status}")
        return running_status

    logger.info("Project version is not running. Attempting to start.")
    return start_project_version(rekog_client, env_vars["project_version_arn"])