import logging
import psycopg2
from pyspark.sql import SparkSession
import utility.stwitter_secrets as sts
import warehouse.stwitter_analytic_queries as staq


def ingest_data() -> None:
    """Creates connection with redshift database and ingests users and tweets data taken from s3 into it."""
    secrets = sts.get_secrets()
    DB_NAME = secrets["DB_NAME"]
    HOST = secrets["HOST"]
    PORT_NAME = secrets["PORT_NAME"]
    USER = secrets["USER"]
    PASSWORD = secrets["PASSWORD"]

    try:
        logging.debug("Connecting to redshift warehouse.")
        wh_conn = psycopg2.connect(
            dbname=DB_NAME, host=HOST, port=PORT_NAME, user=USER, password=PASSWORD
        )
        wh_conn.autocommit = True
        cur = wh_conn.cursor()

        try:
            logging.debug(
                "Executing ingest_tweets_aapl_sentiments_minute_sum_query query."
            )
            cur.execute(staq.ingest_tweets_aapl_sentiments_minute_sum_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Executing ingest_tickers_aapl_minute_avg_query query.")
            cur.execute(staq.ingest_tickers_aapl_minute_avg_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Executing ingest_tickers_aapl_query query.")
            cur.execute(staq.ingest_tickers_aapl_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug(
                "Executing ingest_tweets_goog_sentiments_minute_sum_query query."
            )
            cur.execute(staq.ingest_tweets_goog_sentiments_minute_sum_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Executing ingest_tickers_goog_minute_avg_query query.")
            cur.execute(staq.ingest_tickers_goog_minute_avg_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Executing ingest_tickers_goog_query query.")
            cur.execute(staq.ingest_tickers_goog_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug(
                "Executing ingest_tweets_amzn_sentiments_minute_sum_query query."
            )
            cur.execute(staq.ingest_tweets_amzn_sentiments_minute_sum_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Executing ingest_tickers_amzn_minute_avg_query query.")
            cur.execute(staq.ingest_tickers_amzn_minute_avg_query)
        except Exception as error:
            logging.error(error)
            raise error

        try:
            logging.debug("Executing ingest_tickers_amzn_query query.")
            cur.execute(staq.ingest_tickers_amzn_query)
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
    spark = SparkSession.builder.appName("analytic_ingesting_app").getOrCreate()
    logging.basicConfig(
        level=logging.DEBUG, format="""%(asctime)s:%(levelname)-8s%(message)s"""
    )

    ingest_data()
    spark.stop()
