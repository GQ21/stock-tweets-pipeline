from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, monotonically_increasing_id, row_number
import demoji
import re


def demoji_text(text: str) -> str:
    """Takes string and decodes unitext emojis."""
    return demoji.replace_with_desc(text, sep=" ")


def clean_text(tweet: str) -> str:
    """Takes text, demoji it and removes unnecessary information."""
    processed_tweet = demoji_text(tweet)
    # remove stock market tickers like $AAPL
    processed_tweet = re.sub(r"\$\w*", "", processed_tweet)
    # remove old style retweet text "RT"
    processed_tweet = re.sub(r"^RT[\s]+", "", processed_tweet)
    # remove hyperlinks
    processed_tweet = re.sub(r"http\S+", "", processed_tweet)
    # remove the hash # sign from the word
    processed_tweet = re.sub(r"#", "", processed_tweet)
    # remove digit characters
    processed_tweet = re.sub("[0-9]", " ", processed_tweet)
    # remove all other special characters
    processed_tweet = re.sub(r"\W", " ", processed_tweet)

    return processed_tweet


def process_data(df_tweets: DataFrame) -> DataFrame:
    """Takes tweets dataframe and process every tweet text by calling clean_text function."""
    clean_tweet_text = udf(lambda x: clean_text(x))
    df_tweets = df_tweets.withColumn(
        "tweets_text_cleaned", clean_tweet_text(df_tweets["tweet_text"])
    )

    return df_tweets


def create_sentiments_df(
    spark: SparkSession, df_tweets: DataFrame, tweets_sentiments: list
) -> DataFrame:
    """Takes tweets dataframe and list with predicted sentiments.Creates dataframe with tweet_id and sentiment columns."""
    df_sentiments = spark.createDataFrame(
        [(tweet,) for tweet in tweets_sentiments], ["sentiment"]
    )
    df_tweets_id = df_tweets.select("tweet_id")

    df_sentiments = df_sentiments.withColumn(
        "row_idx", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    df_tweets_id = df_tweets_id.withColumn(
        "row_idx", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    df_sentiments = df_tweets_id.join(
        df_sentiments, df_tweets_id.row_idx == df_sentiments.row_idx
    ).drop("row_idx")

    return df_sentiments
