from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import max as max_, min as min_
import logging
from datetime import datetime, timedelta
import pytz


def check_duplicates(df: DataFrame, column: str) -> None:
    """Takes dataframe and column, checks if there are any duplicates."""
    if df.count() > df.dropDuplicates([column]).count():
        raise ValueError(f"Data column {column} has duplicates")


def check_stocks_order(df_stocks: DataFrame) -> None:
    """Takes stocks dataframe and checks if columns order are correct."""
    stocks_columns_order = [
        "id",
        "price",
        "change_percent",
        "day_volume",
        "change",
        "datetime",
    ]
    stocks_columns = df_stocks.columns
    if stocks_columns_order != stocks_columns:
        logging.debug(f"Different order found : {stocks_columns}")
        raise ValueError("Stocks dataframe has different column order")


def check_tweets_order(df_tweets: DataFrame) -> None:
    """Takes tweets dataframe and checks if columns order are correct."""
    tweets_columns_order = [
        "tweet_id",
        "user_id",
        "tweet_created_at",
        "tweet_source",
        "tweet_retweet_count",
        "tweet_favorite_count",
        "tweet_text",
        "ticker_ids",
    ]
    tweets_columns = df_tweets.columns
    if tweets_columns_order != tweets_columns:
        logging.debug(f"Different order found : {tweets_columns}")
        raise ValueError("Tweets dataframe has different column order")


def check_users_order(df_users: DataFrame) -> None:
    """Takes users dataframe and checks if columns order are correct."""
    users_columns_order = [
        "user_id",
        "user_name",
        "user_created_at",
        "user_location",
        "user_description",
        "user_followers_count",
        "user_statuses_count",
        "user_varified",
    ]
    users_columns = df_users.columns
    if users_columns_order != users_columns:
        logging.debug(f"Different order found: {users_columns}")
        raise ValueError("Users dataframe has different column order")


def check_sentiments_order(df_sentiments: DataFrame) -> None:
    """Takes sentiments dataframe and checks if columns order are correct."""
    sentiments_columns_order = ["tweet_id", "sentiment"]
    sentiments_columns = df_sentiments.columns
    if sentiments_columns_order != sentiments_columns:
        logging.debug(f"Different order found: {sentiments_columns}")
        raise ValueError("Sentiments dataframe has different column order")


def check_stocks_data_types(df_stocks: DataFrame) -> None:
    """Takes stocks dataframe and checks if columns data types are correct."""
    dtypes = df_stocks.dtypes
    correct_dtypes = {
        "id": "string",
        "price": "double",
        "change_percent": "double",
        "day_volume": "int",
        "change": "double",
        "datetime": "timestamp",
    }
    for column in dtypes:
        if column[1] != correct_dtypes[column[0]]:
            raise ValueError(
                f"Stocks dataframe column '{column[0]}' has different data type: {column[1]}"
            )


def check_tweets_data_types(df_tweets: DataFrame) -> None:
    """"Takes tweets dataframe and checks if columns data types are correct."""
    dtypes = df_tweets.dtypes
    correct_dtypes = {
        "tweet_id": "bigint",
        "user_id": "bigint",
        "tweet_created_at": "timestamp",
        "tweet_source": "string",
        "tweet_retweet_count": "int",
        "tweet_favorite_count": "int",
        "tweet_text": "string",
        "ticker_ids": "string",
    }
    for column in dtypes:
        if column[1] != correct_dtypes[column[0]]:
            raise ValueError(
                f"Tweets dataframe column '{column[0]}' has different data type: {column[1]}"
            )


def check_users_data_types(df_users: DataFrame) -> None:
    """"Takes users dataframe and checks if columns data types are correct."""
    dtypes = df_users.dtypes
    correct_dtypes = {
        "user_id": "bigint",
        "user_name": "string",
        "user_created_at": "timestamp",
        "user_location": "string",
        "user_description": "string",
        "user_followers_count": "int",
        "user_statuses_count": "int",
        "user_varified": "boolean",
    }
    for column in dtypes:
        if column[1] != correct_dtypes[column[0]]:
            raise ValueError(
                f"Users dataframe column '{column[0]}' has different data type: {column[1]}"
            )


def check_sentiments_data_types(df_sentiments: DataFrame) -> None:
    """"Takes sentiments dataframe and checks if columns data types are correct."""
    dtypes = df_sentiments.dtypes
    correct_dtypes = {"tweet_id": "bigint", "sentiment": "string"}
    for column in dtypes:
        if column[1] != correct_dtypes[column[0]]:
            raise ValueError(
                f"Users dataframe column '{column[0]}' has different data type: {column[1]}"
            )


def check_date_range(df: DataFrame, column: str, date: tuple) -> None:
    """Takes dataframe and column which contains datetime. Checks if column datetime is in yesterday time range."""
    year, month, day = date

    min_date, max_date = df.select(min_(column), max_(column)).first()
    min_date = min_date.astimezone(pytz.timezone("America/New_York")).replace(
        tzinfo=None
    )
    max_date = max_date.astimezone(pytz.timezone("America/New_York")).replace(
        tzinfo=None
    )

    yesterday_datetime = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
    today_datetime = yesterday_datetime + timedelta(days=1)

    if min_date < yesterday_datetime:
        raise ValueError(
            f"Column '{column}' oldest {min_date} date is older than yesterday date {yesterday_datetime}"
        )
    elif max_date >= today_datetime:
        raise ValueError(
            f"Column '{column}' newest {max_date} date is newer than today date {today_datetime}"
        )
