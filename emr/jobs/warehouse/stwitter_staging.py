def get_csv_path(s3_client, bucket: str, date: tuple, type: str, folder=None) -> str:
    """Takes csv dataframe location information, combines them to get csv file path."""
    year, month, day = date
    if folder:
        location_prefix = f"{type}/{year}/{month}/{day}/{folder}"
    else:
        location_prefix = f"{type}/{year}/{month}/{day}"

    list_of_files = s3_client.list_objects(Bucket=bucket, Prefix=location_prefix)[
        "Contents"
    ]
    if len(list_of_files) == 1:
        csv_path = list_of_files[0]["Key"]
    elif len(list_of_files) == 0:
        print(f"No csv files found in {bucket}/{location_prefix}")
        raise
    else:
        print(f"Multiple csv files found in {bucket}/{location_prefix}")
        raise
    return csv_path


def ingest_query(
    staging_schema: str, csv_path: str, table_name: str, iam_role: str
) -> str:
    """Creates query which can be used to ingest tweets data  into tweets table with found tweets processed csv file."""

    ingest_tweets_query = f"COPY {staging_schema}.{table_name} FROM 's3://stwit-processed-bucket/{csv_path}' IAM_ROLE '{iam_role}' CSV DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'auto';"
    return ingest_tweets_query
