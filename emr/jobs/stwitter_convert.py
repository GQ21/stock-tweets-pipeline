from pyspark.sql import SparkSession
import logging
import utility.stwitter_date as std
import utility.stwitter_data_migration as stdm
import convertion.stwitter_stringify as sts


def stocks_csv_convert(
    s3_landing_bucket: str, s3_working_bucket: str, date: tuple
) -> None:
    """
    Converts stocks json data taken from s3 landing bucket to csv format
    and exports it to working bucket.
    """
    columns = [
        "id as id",
        "price as price",
        "timestamp as timestamp",
        "changePercent as change_percent",
        "dayVolume as day_volume",
        "change as change",
    ]

    logging.debug("Start reading stocks data.")
    df_stocks = stdm.read_json(spark, s3_landing_bucket, date, "stocks")
    df_stocks = df_stocks.selectExpr(columns)

    logging.debug("Calling export_stocks_csv function.")
    stdm.export_csv(df_stocks, s3_working_bucket, date, "stocks")


def twitter_csv_convert(
    s3_landing_bucket: str, s3_working_bucket: str, date: tuple
) -> None:
    """
    Converts tweets json data taken from s3 landing bucket to csv format
    and exports it to working bucket.
    """

    columns = [
        "id as tweet_id",
        "created_at as tweet_created_at",
        "source as tweet_source",
        "retweet_count as tweet_retweet_count",
        "favorite_count as tweet_favorite_count",
        "text as tweet_text",
        "extended_tweet.full_text as tweet_full_text",
        "entities.symbols as symbols",
        "user.id as user_id",
        "user.name as user_name",
        "user.created_at as user_created_at",
        "user.location as user_location",
        "user.description as user_description",
        "user.followers_count as user_followers_count",
        "user.statuses_count as user_statuses_count",
        "user.verified as user_varified",
    ]

    tweets_columns = [
        "tweet_id",
        "user_id",
        "tweet_created_at",
        "tweet_source",
        "tweet_retweet_count",
        "tweet_favorite_count",
        "tweet_text",
        "tweet_full_text",
        "symbols",
    ]
    user_columns = [
        "user_id",
        "user_name",
        "user_created_at",
        "user_location",
        "user_description",
        "user_followers_count",
        "user_statuses_count",
        "user_varified",
    ]
    logging.debug("Start reading twitter data.")
    df_tweets_all = stdm.read_json(spark, s3_landing_bucket, date, "twitter")
    df_tweets_all = df_tweets_all.selectExpr(columns)

    logging.debug("Selecting tweets columns.")
    df_tweets = df_tweets_all.select(tweets_columns)

    logging.debug("Calling stringify_column function.")
    df_tweets = sts.stringify_column(df_tweets, "symbols")

    logging.debug("Selecting user columns.")
    df_users = df_tweets_all.select(user_columns)

    logging.debug("Calling export_twitter_csv function with df_tweets dataframe.")
    stdm.export_csv(df_tweets, s3_working_bucket, date, "twitter", "tweets")

    logging.debug("Calling export_twitter_csv function with df_users dataframe.")
    stdm.export_csv(df_users, s3_working_bucket, date, "twitter", "users")


def convert_data() -> None:
    """Gets yesterday date and runs stocks_csv_convert and twitter_csv_convert functions."""
    yesterday_date = std.get_yesterday()
    s3_working_bucket = "stwit-working-bucket"
    s3_landing_bucket = "stwit-landing-bucket"

    stocks_csv_convert(s3_landing_bucket, s3_working_bucket, yesterday_date)
    twitter_csv_convert(s3_landing_bucket, s3_working_bucket, yesterday_date)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("working_app").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")

    logging.basicConfig(
        level=logging.DEBUG, format="""%(asctime)s:%(levelname)-8s%(message)s"""
    )
    convert_data()
    spark.stop()
