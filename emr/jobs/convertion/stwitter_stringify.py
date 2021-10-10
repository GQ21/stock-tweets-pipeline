from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


def array_to_string(row_list: list) -> str:
    """Takes list and stringify it."""
    return "[" + ",".join([str(elem) for elem in row_list]) + "]"


def stringify_column(df: DataFrame, column: str) -> DataFrame:
    """Takes dataframe and column that contains array structures. Stringify that column values."""
    array_to_string_udf = udf(array_to_string, StringType())
    df = df.withColumn(column, array_to_string_udf(df[column]))
    return df
