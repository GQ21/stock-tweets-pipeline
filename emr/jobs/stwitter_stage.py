import boto3
import psycopg2
import logging
from pyspark.sql import SparkSession
import utility.stwitter_secrets as sts
import utility.stwitter_date as std
import warehouse.stwitter_staging as wrs


def ingest_data() -> None:
    """Creates connection with redshift database and into it ingests tweets,users,sentiments,stocks data taken from s3."""
    yesterday_date = std.get_yesterday()
    staging_schema = "stwitter_staging"
    s3_processed_bucket = "stwit-processed-bucket"

    secrets = sts.get_secrets()
    DB_NAME = secrets["DB_NAME"]
    HOST = secrets["HOST"]
    PORT_NAME = secrets["PORT_NAME"]
    USER = secrets["USER"]
    PASSWORD = secrets["PASSWORD"]
    IAM_ROLE = secrets["IAM_ROLE"]

    try:
        logging.debug("Connecting to redshift warehouse.")
        wh_conn = psycopg2.connect(
            dbname=DB_NAME, host=HOST, port=PORT_NAME, user=USER, password=PASSWORD
        )
        cur = wh_conn.cursor()

        try:
            logging.debug("Calling get_csv_path function to find tweets csv path.")
            tweets_csv_path = wrs.get_csv_path(
                s3_client, s3_processed_bucket, yesterday_date, "twitter", "tweets"
            )

            logging.debug(
                "Calling ingest_query function to get tweets table ingestion query."
            )
            ingest_tweets_query_str = wrs.ingest_query(
                staging_schema, tweets_csv_path, "tweets", IAM_ROLE
            )

            logging.debug("Executing ingest_tweets_query_str query.")
            cur.execute(ingest_tweets_query_str)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Calling get_csv_path function to find users csv path.")
            users_csv_path = wrs.get_csv_path(
                s3_client, s3_processed_bucket, yesterday_date, "twitter", "users"
            )

            logging.debug(
                "Calling ingest_query function to get users table ingestion query."
            )
            ingest_users_query_str = wrs.ingest_query(
                staging_schema, users_csv_path, "users", IAM_ROLE
            )

            logging.debug("Executing ingest_users_query_str query.")
            cur.execute(ingest_users_query_str)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Calling get_csv_path function to find stocks csv path.")
            stocks_csv_path = wrs.get_csv_path(
                s3_client, s3_processed_bucket, yesterday_date, "stocks"
            )

            logging.debug(
                "Calling ingest_query function to get users table ingestion query."
            )
            ingest_stocks_query_str = wrs.ingest_query(
                staging_schema, stocks_csv_path, "tickers", IAM_ROLE
            )

            logging.debug("Executing ingest_stocks_query_str query.")
            cur.execute(ingest_stocks_query_str)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Calling get_csv_path function to find sentiments csv path.")
            sentiments_csv_path = wrs.get_csv_path(
                s3_client, s3_processed_bucket, yesterday_date, "sentiments"
            )

            logging.debug(
                "Calling ingest_query function to get sentiments table ingestion query."
            )
            ingest_sentiments_query_str = wrs.ingest_query(
                staging_schema, sentiments_csv_path, "sentiments", IAM_ROLE
            )

            logging.debug("Executing ingest_sentiments_query_str query.")
            cur.execute(ingest_sentiments_query_str)
        except Exception as error:
            logging.error(error)
            raise error

        logging.debug("Commiting cursor execution.")
        wh_conn.commit()

        wh_conn.close()
        cur.close()
    except psycopg2.DatabaseError as error:
        logging.error(error)
        raise (error)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("staging_ingesting_app").getOrCreate()
    s3_client = boto3.client("s3")
    logging.basicConfig(
        level=logging.DEBUG, format="""%(asctime)s:%(levelname)-8s%(message)s"""
    )

    ingest_data()
    spark.stop()
