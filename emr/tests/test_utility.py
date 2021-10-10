import emr.utility.stwitter_secrets as sts
import emr.utility.stwitter_date as std
import emr.utility.stwitter_data_migration as stdm
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
import boto3


def test_get_secrets_type() -> None:
    secrets = sts.get_secrets()
    assert type(secrets) == dict


def test_get_secrets_keys() -> None:
    secrets = sts.get_secrets()
    keys = [
        "DB_NAME",
        "HOST",
        "PORT_NAME",
        "USER",
        "PASSWORD",
        "IAM_ROLE",
        "S3_ACCESS_KEY",
        "S3_SECRET_ACCESS_KEY",
    ]
    for key in keys:
        assert key in secrets.keys()


def test_get_yesterday_len() -> None:
    yesterday = std.get_yesterday()
    assert len(yesterday) == 3


def test_load_model() -> None:
    s3 = boto3.resource("s3")
    bucket = "stwit-jobs"
    model = stdm.load_model(s3, bucket)
    assert type(model) == LogisticRegression


def test_load_vectorize() -> None:
    s3 = boto3.resource("s3")
    bucket = "stwit-jobs"
    vectorizer = stdm.load_vectorizer(s3, bucket)
    assert type(vectorizer) == TfidfVectorizer
