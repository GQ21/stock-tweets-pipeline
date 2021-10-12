import pytest
from datetime import datetime
import emr.quality.stwitter_quality_checks as stqc
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("test_quality_checks_app").getOrCreate()
df_test = spark.createDataFrame(
    [(1, "test_a"), (2, "test_a"), (3, "test_b")], ["id", "test_column"]
)


def test_check_duplicates() -> None:
    with pytest.raises(ValueError):
        stqc.check_duplicates(df_test, "test_column")


def test_check_stocks_order() -> None:
    with pytest.raises(ValueError):
        stqc.check_stocks_order(df_test)


def test_check_tweets_order() -> None:
    with pytest.raises(ValueError):
        stqc.check_tweets_order(df_test)


def test_check_users_order() -> None:
    with pytest.raises(ValueError):
        stqc.check_users_order(df_test)


def test_check_sentiments_order() -> None:
    with pytest.raises(ValueError):
        stqc.check_sentiments_order(df_test)


def test_check_stocks_data_types() -> None:
    df_test_stocks = spark.createDataFrame(
        [
            (1, "test_a"),
            (2, "test_a"),
        ],
        ["id", "price"],
    )
    with pytest.raises(ValueError):
        stqc.check_stocks_data_types(df_test_stocks)


def test_tweets_data_types() -> None:
    df_test_tweets = spark.createDataFrame(
        [
            ("test_a", "test_a"),
            ("test_a", "test_a"),
        ],
        ["tweet_id", "user_id"],
    )
    with pytest.raises(ValueError):
        stqc.check_tweets_data_types(df_test_tweets)


def test_check_users_data_types() -> None:
    df_test_users = spark.createDataFrame(
        [
            ("test_a", 2),
            ("test_a", 1),
        ],
        ["user_id", "user_name"],
    )
    with pytest.raises(ValueError):
        stqc.check_users_data_types(df_test_users)


def test_check_sentiments_data_types() -> None:
    df_test_sentiments = spark.createDataFrame(
        [
            ("test_a", 2),
            ("test_a", 1),
        ],
        ["tweet_id", "sentiment"],
    )
    with pytest.raises(ValueError):
        stqc.check_sentiments_data_types(df_test_sentiments)


def test_check_date_range_older() -> None:
    df_test_date = spark.createDataFrame(
        [
            (datetime(2021, 10, 1), 2),
        ],
        "date timestamp, id int",
    )
    date = ("2021", "10", "2")
    with pytest.raises(ValueError):
        stqc.check_date_range(df_test_date, "date", date)


def test_check_date_range_newer() -> None:
    df_test_date = spark.createDataFrame(
        [
            (datetime(2021, 10, 4), 2),
        ],
        "date timestamp, id int",
    )
    date = ("2021", "10", "2")
    with pytest.raises(ValueError):
        stqc.check_date_range(df_test_date, "date", date)
