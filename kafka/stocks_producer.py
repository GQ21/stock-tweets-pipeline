import json
from kafka import KafkaProducer
import yliveticker
import logging
import time

logging.basicConfig(
    filename="stocks_producer.log",
    level=logging.INFO,
    format="""%(asctime)s:%(levelname)-8s%(message)s""",
)


def json_serializer(data: dict) -> dict:
    """Takes input data and dumps it with json utf-8 encoded format """
    return json.dumps(data).encode("utf-8")


def on_new_msg(ws, message: dict) -> None:
    print(message)
    producer.send(topic_name, message)


topic_name = "stocks_stream"
logging.info("Initializing kafka producer.")
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"], value_serializer=json_serializer
)


def start_yahoo_streaming(tickers: list) -> None:
    """Recursive function that starts yahoo finance streaming and if error occurs restarts itself."""
    logging.info(f"Starting streaming with {tickers} casthags.")
    yliveticker.YLiveTicker(on_ticker=on_new_msg, ticker_names=tickers)

    logging.error(f"Streaming stopped.")
    time.sleep(2)
    logging.info("Restarting streaming.")
    print("Restarting streaming.")
    start_yahoo_streaming(tickers)


tickers = ["AAPL", "GOOG", "AMZN"]
start_yahoo_streaming(tickers)
