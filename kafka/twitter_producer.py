from dotenv import load_dotenv
import os

load_dotenv()

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import time
from kafka import KafkaProducer
import logging

logging.basicConfig(
    filename="twitter_producer.log",
    level=logging.INFO,
    format="""%(asctime)s:%(levelname)-8s%(message)s""",
)

CONSUMER_KEY = os.environ["CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["CONSUMER_SECRET"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = os.environ["ACCESS_TOKEN_SECRET"]


def json_serializer(data: dict) -> dict:
    """Takes input data and dumps it with json utf-8 encoded format """
    return json.dumps(data).encode("utf-8")


class StdOutListener(StreamListener):
    def on_data(self, data: dict) -> None:
        if "text" and "symbols" in data:
            print(data)
            producer.send(topic_name, data)

    def on_error(self, status: str) -> None:

        logging.error(f"Streaming stopped because of error: \n{status}")


topic_name = "twitter_stream"
logging.info("Initializing kafka producer.")
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"], value_serializer=json_serializer
)

logging.info("Initializing StdOutListener.")
listener = StdOutListener()
logging.info("Authenticating twitter api access.")
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


def start_twitter_streaming(cashtags: list) -> None:
    """Recursive function that starts twitter streaming and if error occurs restarts itself."""
    try:
        logging.info(f"Starting streaming with {cashtags} casthags.")
        stream = Stream(auth, listener)
        stream.filter(track=cashtags)
    except Exception as e:
        logging.error(f"Streaming stopped because of error: \n{e}")
        time.sleep(2)
        logging.info("Restarting streaming.")
        print("Restarting streaming.")
        start_twitter_streaming(cashtags)


cashtags = ["$AAPL", "$GOOG", "$AMZN"]
start_twitter_streaming(cashtags)
