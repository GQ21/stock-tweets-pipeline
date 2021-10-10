from datetime import datetime, timedelta
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    regexp_extract,
    col,
    to_timestamp,
    when,
    udf,
    from_utc_timestamp,
)
import re
import pytz


def gmt_unix_to_datetime(df: DataFrame, column: str) -> DataFrame:
    """Takes dataframe, column and converts given unix GMT time column to EST datetime format."""
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
    )
    df = df.withColumn("datetime_gmt", get_timestamp(df[column]))
    df = df.withColumn(
        "datetime", from_utc_timestamp(df["datetime_gmt"], "America/New_York")
    )
    df = df.drop("datetime_gmt", column)
    return df


def extract_tweet_source(df: DataFrame) -> DataFrame:
    """Takes dataframe and extracts tweet source names from tweet_source column."""
    exp = "(?<=>).*(?=<)"
    df = df.withColumn("tweet_source", regexp_extract(col("tweet_source"), exp, 0))
    return df


def col_to_datetime(df: DataFrame, column: str) -> DataFrame:
    """Takes dataframe, column and converts that column to date timestamp format."""
    df = df.withColumn(
        column, to_timestamp(col(column), "EEE MMM dd HH:mm:ss zzz yyyy")
    )
    return df


def merge_texts(df: DataFrame) -> DataFrame:
    """
    Takes dataframe and merges two columns tweet_full_text and tweet_text into one text column
    depending on wheter there is tweet_full_text value it takes tweet_full_text value if not takes
    tweet_text value.
    """
    df = df.withColumn(
        "tweet_text",
        when(df["tweet_full_text"].isNull(), df["tweet_text"]).otherwise(
            df["tweet_full_text"]
        ),
    )
    df = df.drop("tweet_full_text")
    return df


def extract_tickers(row_value: str) -> str:
    """
    Takes string value, search for tickers between quotes and return all found tickers
    sperated with commas if no tickers found return None.
    """
    tickers = re.findall("'(.*?)'", row_value)
    tickers_str = ""
    if len(tickers) != 0:
        for ticker in tickers:
            if len(tickers_str) == 0:
                ticker = ticker.upper()
                tickers_str = tickers_str + str(ticker)
            else:
                ticker = ticker.upper()
                tickers_str = tickers_str + "," + str(ticker)
    else:
        return None
    return tickers_str


def get_tickers(df: DataFrame) -> DataFrame:
    """
    Takes daframe, creates column ticker_ids from found tickers in symbols column.
    Drops symbol column.
    """
    get_tickers_udf = udf(lambda x: extract_tickers(x))
    df = df.withColumn("ticker_ids", get_tickers_udf(df["symbols"]))
    df = df.drop("symbols")
    return df


def remove_duplicate_tweets(df_tweets: DataFrame, df_users: DataFrame) -> tuple:
    """
    Takes tweets and users dataframes and remove duplicates.
    """
    df_users = df_users.join(df_tweets.select("user_id", "tweet_id"), ["user_id"])
    df_users = df_users.drop_duplicates(["tweet_id"])
    df_users = df_users.drop("tweet_id")

    df_tweets = df_tweets.drop_duplicates(["tweet_id"])
    return df_tweets, df_users


def filter_tweets(df_tweets: DataFrame, df_users: DataFrame) -> tuple:
    """
    Takes tweets and users dataframes and filters it depending on whether ticker_ids column value
    is not null or null.
    """
    df_users = df_users.join(df_tweets.select("user_id", "ticker_ids"), ["user_id"])
    df_users = df_users.filter(df_users["ticker_ids"].isNotNull())
    df_users = df_users.drop("ticker_ids")

    df_tweets = df_tweets.filter(df_tweets["ticker_ids"].isNotNull())
    return df_tweets, df_users


def order_by_col(df: DataFrame, column: str) -> DataFrame:
    """Orders dataframe depending on given column in ascending manner."""
    df = df.sort(col(column).asc())
    return df


def drop_outofrange(df: DataFrame, column: str, date: tuple) -> DataFrame:
    """Drops out rows that are out of yesterday's date range."""
    year, month, day = date
    yesterday_datetime = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
    tz = pytz.timezone("America/New_York")
    yesterday_datetime = tz.localize(yesterday_datetime)
    today_datetime = yesterday_datetime + timedelta(days=1)

    df = df.filter((col(column) >= yesterday_datetime) & (col(column) < today_datetime))
    return df
