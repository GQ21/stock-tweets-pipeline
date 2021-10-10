from pyspark.sql import SparkSession
import logging
import utility.stwitter_date as std
import utility.stwitter_data_migration as stdm
import quality.stwitter_quality_checks as stqc


def check_stocks_quality(s3_processed_bucket: str, date: tuple) -> None:
    """Loads stocks dataframe and calls multiple quality check functions."""

    logging.debug("Calling load_stocks_data function.")
    df_stocks = stdm.read_csv(spark, s3_processed_bucket, date, "stocks")

    logging.debug("Calling check_stocks_order function.")
    stqc.check_stocks_order(df_stocks)

    logging.debug("Calling check_stocks_data_types function.")
    stqc.check_stocks_data_types(df_stocks)

    logging.debug(
        "Calling check_date_range function with df_stocks dataframe and 'datetime' column."
    )
    stqc.check_date_range(df_stocks, "datetime", date)


def check_tweets_quality(s3_processed_bucket: str, date: tuple) -> None:
    """Loads tweets dataframe and calls multiple quality check functions."""

    logging.debug("Calling load_tweets_data function.")
    df_tweets = stdm.read_csv(spark, s3_processed_bucket, date, "twitter", "tweets")

    logging.debug(
        "Calling check_duplicates function with df_tweets dataframe and 'tweet_id' column."
    )
    stqc.check_duplicates(df_tweets, "tweet_id")

    logging.debug("Calling check_tweets_order function.")
    stqc.check_tweets_order(df_tweets)

    logging.debug("Calling check_tweets_data_types function.")
    stqc.check_tweets_data_types(df_tweets)

    logging.debug(
        "Calling check_date_range function with df_tweets dataframe and 'tweet_created_at' column."
    )
    stqc.check_date_range(df_tweets, "tweet_created_at", date)


def check_users_quality(s3_processed_bucket: str, date: tuple) -> None:
    """Loads users dataframe and calls multiple quality check functions."""

    logging.debug("Calling load_users_data function.")
    df_users = stdm.read_csv(spark, s3_processed_bucket, date, "twitter", "users")

    logging.debug("Calling check_users_order function.")
    stqc.check_users_order(df_users)

    logging.debug("Calling check_users_data_types function.")
    stqc.check_users_data_types(df_users)


def check_sentiments_quality(s3_processed_bucket: str, date: tuple) -> None:
    """Loads sentiments dataframe and calls multiple quality check functions."""

    logging.debug("Calling load_users_data function.")
    df_sentiments = stdm.read_csv(spark, s3_processed_bucket, date, "sentiments")

    logging.debug(
        "Calling check_duplicates function with df_sentiments dataframe and 'tweet_id' column."
    )
    stqc.check_duplicates(df_sentiments, "tweet_id")

    logging.debug("Calling check_sentiments_order function.")
    stqc.check_sentiments_order(df_sentiments)

    logging.debug("Calling check_sentiments_data_types function.")
    stqc.check_sentiments_data_types(df_sentiments)


def check_quality() -> None:
    """Finds yesterday date and calls stocks,tweets,users,sentiments quality functions."""
    yesterday_date = std.get_yesterday()
    s3_processed_bucket = "stwit-processed-bucket"

    check_stocks_quality(s3_processed_bucket, yesterday_date)
    check_tweets_quality(s3_processed_bucket, yesterday_date)
    check_users_quality(s3_processed_bucket, yesterday_date)
    check_sentiments_quality(s3_processed_bucket, yesterday_date)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("quality_app").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")

    logging.basicConfig(
        level=logging.DEBUG, format="""%(asctime)s:%(levelname)-8s%(message)s"""
    )
    check_quality()
    spark.stop()
