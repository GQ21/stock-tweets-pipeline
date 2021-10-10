from datetime import datetime, timedelta
import pytz
import logging


def get_yesterday() -> tuple:
    """Get yesterday`s date and split it to year,month and day strings"""
    logging.debug("Starting get_yesterday function.")
    today = datetime.now(pytz.timezone("America/New_York"))
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday_split = yesterday.split("-")
    year = yesterday_split[0]
    month = yesterday_split[1]
    day = yesterday_split[2]

    return year, month, day
