import logging
from pyspark.sql import SparkSession
import utility.stwitter_date as std
import utility.stwitter_data_migration as stdm
import process.stwitter_processing as stp
from pyspark.sql.dataframe import DataFrame


def process_users(s3_working_bucket: str, date: tuple) -> DataFrame:
    """Process users csv file taken from s3 working bucket"""

    logging.debug("Start reading users csv.")
    df_users = stdm.read_csv(spark, s3_working_bucket, date, "twitter", "users")

    logging.debug("Calling col_to_datetime function with df_users data.")
    df_users = stp.col_to_datetime(df_users, "user_created_at")

    return df_users


def process_tweets(s3_working_bucket: str, date: tuple) -> DataFrame:
    """Process tweets csv file taken from s3 working bucket."""

    logging.debug("Start reading tweets csv.")
    df_tweets = stdm.read_csv(spark, s3_working_bucket, date, "twitter", "tweets")

    logging.debug("Calling extract_tweet_source function.")
    df_tweets = stp.extract_tweet_source(df_tweets)

    logging.debug("Calling col_to_datetime function with df_tweets data.")
    df_tweets = stp.col_to_datetime(df_tweets, "tweet_created_at")

    logging.debug("Calling merge_texts function.")
    df_tweets = stp.merge_texts(df_tweets)

    logging.debug("Calling get_tickers function.")
    df_tweets = stp.get_tickers(df_tweets)

    # In case json files are loaded not in order.
    logging.debug("Calling order_by_col function with df_tweets data.")
    df_tweets = stp.order_by_col(df_tweets, "tweet_created_at")

    logging.debug("Calling drop_outofrange function with df_tweets data.")
    df_tweets = stp.drop_outofrange(df_tweets, "tweet_created_at", date)

    return df_tweets


def process_stocks(s3_working_bucket: str, date: tuple) -> DataFrame:
    """Processes stocks data taken from s3 working bucket"""

    logging.debug("Start reading stocks csv.")
    df_stocks = stdm.read_csv(spark, s3_working_bucket, date, "stocks")

    logging.debug("Calling gmt_unix_to_datetime function.")
    df_stocks = stp.gmt_unix_to_datetime(df_stocks, "timestamp")

    logging.debug("Calling order_by_col function.")
    df_stocks = stp.order_by_col(df_stocks, "datetime")

    return df_stocks


def process_data() -> None:
    """
    Calls process functions to process tweets,users,stocks csvs.Filters twitter,users data.
    Exports all csvs to processed s3 bucket.
    """
    yesterday_date = std.get_yesterday()
    s3_working_bucket = "stwit-working-bucket"
    s3_processed_bucket = "stwit-processed-bucket"

    df_tweets = process_tweets(s3_working_bucket, yesterday_date)
    df_users = process_users(s3_working_bucket, yesterday_date)
    df_stocks = process_stocks(s3_working_bucket, yesterday_date)

    logging.debug("Calling filter_tweets function.")
    df_tweets, df_users = stp.filter_tweets(df_tweets, df_users)

    logging.debug("Calling remove_duplicate_tweets function.")
    df_tweets, df_users = stp.remove_duplicate_tweets(df_tweets, df_users)

    logging.debug("Calling export_twitter_csv function with df_tweets data.")
    stdm.export_csv(df_tweets, s3_processed_bucket, yesterday_date, "twitter", "tweets")

    logging.debug("Calling export_twitter_csv function with df_users data.")
    stdm.export_csv(df_users, s3_processed_bucket, yesterday_date, "twitter", "users")

    logging.debug("Calling export_stocks_csv function.")
    stdm.export_csv(df_stocks, s3_processed_bucket, yesterday_date, "stocks")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("processing_app").getOrCreate()
    # For spark>3
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")

    logging.basicConfig(
        level=logging.DEBUG, format="""%(asctime)s:%(levelname)-8s%(message)s"""
    )

    process_data()
    spark.stop()
