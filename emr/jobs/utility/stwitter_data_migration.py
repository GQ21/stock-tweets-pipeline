from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import pickle
from boto3.resources.factory import ServiceResource


def export_csv(
    dataframe: DataFrame, bucket: str, date: tuple, type: str, folder=None
) -> None:
    """Takes twitter dataframe and exports it to provided path information."""
    year, month, day = date
    if folder:
        path = f"s3a://{bucket}/{type}/{year}/{month}/{day}/{folder}"
    else:
        path = f"s3a://{bucket}/{type}/{year}/{month}/{day}"

    dataframe.coalesce(1).write.option(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    ).csv(path, header=True, mode="overwrite", quote='"', escape='"')


def read_json(spark: SparkSession, bucket: str, date: tuple, type: str) -> DataFrame:
    """Reads json data from provided path information."""
    year, month, day = date
    path = f"s3a://{bucket}/{type}/{year}/{month}/{day}"
    df = spark.read.json(path)

    return df


def read_csv(
    spark: SparkSession, bucket: str, date: tuple, type: str, folder=None
) -> None:
    """Takes csv path and spark reads dataframe."""
    year, month, day = date
    if folder:
        path = f"s3a://{bucket}/{type}/{year}/{month}/{day}/{folder}"
    else:
        path = f"s3a://{bucket}/{type}/{year}/{month}/{day}"

    df = spark.read.option("multiline", "true").csv(
        path,
        header=True,
        mode="PERMISSIVE",
        inferSchema=True,
        quote='"',
        escape='"',
    )
    return df


def load_model(s3: ServiceResource, bucket: str):
    """With pickle loads sklearn logistic regressiom model from s3 bucket."""
    logistic_reg = pickle.loads(
        s3.Bucket(bucket).Object("logistic_reg.pkl").get()["Body"].read()
    )
    return logistic_reg


def load_vectorizer(s3: ServiceResource, bucket: str):
    """With pickle loads sklearn TfidfVectorizer from s3 bucket."""
    vectorizer = pickle.loads(
        s3.Bucket(bucket).Object("vectorizer.pkl").get()["Body"].read()
    )
    return vectorizer
