from pyspark import SparkContext
import pytest
from pyspark.sql import SparkSession, SparkSession, DataFrame
from awsglue import DynamicFrame
from framework.data_frame_matcher import DataFrameMatcher


@pytest.fixture(scope='session')
def spark_context():
    # TODO: move me when the fixtures is merged
    spark = SparkSession.builder.master('local[1]').getOrCreate()
    yield spark.sparkContext


@pytest.fixture()
def spark_session(spark_context: SparkContext):
    yield SparkSession(spark_context)


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
