from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, lit


def merge_and_retain_last(
    df1: DataFrame,
    df2: DataFrame,
    key_fields: List[str] = [],
    sort_field: str = None,
    deleted_field: str = None,
) -> DataFrame:
    """
    Merges the provided data frames together and returns a new DataFrame that contains only the most recent record
    for each value within the given `key_fields`

    key_fields : List[str]
        A list of fields that uniquely identify the entity being merged
    sort_field : str
        A field (usually a timestamp) to use for determining what record to keep. The record with the greatest value is retained.
    deleted_field : str
        The field to use as a deleted indicator. If this is True then the row is considered a delete marker.
    """
    window = Window.partitionBy(key_fields).orderBy(col(sort_field).desc())

    def add_deleted_flag_if_needed(df: DataFrame) -> DataFrame:
        if deleted_field not in df.columns:
            return df.withColumn(deleted_field, lit(True))
        return df

    row_num_col = "__row_num"

    df1 = add_deleted_flag_if_needed(df1)
    df2 = add_deleted_flag_if_needed(df2)
    merged = (
        df1.unionByName(df2)
        .select("*", row_number().over(window).alias(row_num_col))
        .filter(col(row_num_col) == 1)
        .filter(col(deleted_field) != True)
        .drop(row_num_col)
    )
    return merged
