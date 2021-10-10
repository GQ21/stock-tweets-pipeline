import boto3
from kafka import KafkaConsumer
import json
import datetime
import pytz
from dotenv import load_dotenv
import os

load_dotenv()

S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_ACCESS_KEY = os.environ["S3_SECRET_ACCESS_KEY"]


def json_deserializer(data: dict) -> dict:
    return json.loads(data)


def get_today() -> tuple:
    """Get today`s date and split it to year,month and day strings"""
    today = datetime.datetime.now(pytz.timezone("America/New_York")).strftime(
        "%Y-%m-%d"
    )
    today_split = today.split("-")
    year = today_split[0]
    month = today_split[1]
    day = today_split[2]

    return today, year, month, day


def get_time() -> str:
    """Gets current time"""
    current_time = datetime.datetime.now(pytz.timezone("America/New_York")).strftime(
        "%H:%M:%S"
    )
    return current_time


def upload(message: dict) -> None:
    """Takes message and uploads it to S3 landing bucket."""
    today, year, month, day = get_today()
    upload_file_bucket = "stwit-landing-bucket"
    current_time = get_time()

    upload_file_key = (
        "twitter/"
        + f"{year}/"
        + f"{month}/"
        + f"{day}/"
        + "tweet_"
        + str(today)
        + "_"
        + str(current_time)
        + ".json"
    )

    client.put_object(Body=message, Bucket=upload_file_bucket, Key=upload_file_key)


topic_name = "twitter_stream"
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    value_deserializer=json_deserializer,
)
client = boto3.client(
    "s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_ACCESS_KEY
)

for message in consumer:
    message = message.value
    print(message)
    upload(message)
