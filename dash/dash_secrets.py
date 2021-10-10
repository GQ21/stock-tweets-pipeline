import boto3
from botocore.exceptions import ClientError
import logging
import json

def get_secrets() -> dict:
    """Connects to aws secret manager and gets secrets."""
    secret_name = "stwitter_secret"
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="eu-north-1")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return json.loads(get_secret_value_response["SecretString"])
    except ClientError as e:
        logging.error(e)
        raise
