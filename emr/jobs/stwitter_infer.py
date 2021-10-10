from pyspark.sql import SparkSession
import logging
import boto3
import utility.stwitter_date as std
import utility.stwitter_data_migration as stdm
import inference.stwitter_inference_process as stip


def predict_data() -> None:
    """Predicts sentiments for processed tweets data and exports predicted data to s3 processed bucket."""
    yesterday_date = std.get_yesterday()
    s3_processed_bucket = "stwit-processed-bucket"
    s3_jobs_bucket = "stwit-jobs"

    logging.debug("Loading logistic regression model.")
    logistic_reg = stdm.load_model(s3, s3_jobs_bucket)

    logging.debug("Loading vectorizer.")
    vectorizer = stdm.load_vectorizer(s3, s3_jobs_bucket)

    logging.debug("Reading tweets data from s3 bucket.")
    df_tweets = stdm.read_csv(
        spark, s3_processed_bucket, yesterday_date, "twitter", "tweets"
    )

    logging.debug("Starting process_data function.")
    df_tweets = stip.process_data(df_tweets)

    logging.debug("Converting df_tweets.tweets_text column into list.")
    tweets_text = [row[0] for row in df_tweets.select("tweets_text_cleaned").collect()]

    logging.debug("Vectorizing tweets text.")
    tweets_vectorized = vectorizer.transform(tweets_text)

    logging.debug("Making prediction from vectorized text.")
    tweets_sentiments = logistic_reg.predict(tweets_vectorized)

    logging.debug("Starting create_sentiment_df function.")
    df_sentiments = stip.create_sentiments_df(spark, df_tweets, tweets_sentiments)

    logging.debug("Starting export_sentiments_cs function.")
    stdm.export_csv(df_sentiments, s3_processed_bucket, yesterday_date, "sentiments")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("predict_app").getOrCreate()
    logging.basicConfig(
        level=logging.DEBUG, format="""%(asctime)s:%(levelname)-8s%(message)s"""
    )
    s3 = boto3.resource("s3")

    predict_data()
    spark.stop()
