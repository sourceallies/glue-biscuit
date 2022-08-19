from pyspark.sql import SparkSession, DataFrame
from framework.test.data_frame_matcher import DataFrameMatcher


def test_equal_when_one_row_column(spark_session: SparkSession):
    right: DataFrame = spark_session.createDataFrame([{"a": 1}])

    matcher = DataFrameMatcher([{"a": 1}])
    assert matcher.__eq__(right)


def test_not_equal_when_column_names_differ(spark_session: SparkSession):
    right: DataFrame = spark_session.createDataFrame([{"a": 1}])

    matcher = DataFrameMatcher([{"b": 1}])
    assert not matcher.__eq__(right)


def test_not_equal_when_column_value_differs(spark_session: SparkSession):
    right: DataFrame = spark_session.createDataFrame([{"a": 1}])

    matcher = DataFrameMatcher([{"a": 2}])
    assert not matcher.__eq__(right)
