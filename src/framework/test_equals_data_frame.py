from pyspark import SparkContext
import pytest
from pyspark.sql import SparkSession, SparkSession, DataFrame
from awsglue import DynamicFrame
from framework.equals_data_frame import EqualDataFrame


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
    
    comparitor = EqualDataFrame([{"a": 1}])
    assert comparitor.__eq__(right)


def test_not_equal_when_column_names_differ(spark_session: SparkSession):
    right: DataFrame = spark_session.createDataFrame([{"a": 1}])
    
    comparitor = EqualDataFrame([{"b": 1}])
    assert not comparitor.__eq__(right)


def test_not_equal_when_column_value_differs(spark_session: SparkSession):
    right: DataFrame = spark_session.createDataFrame([{"a": 1}])
    
    comparitor = EqualDataFrame([{"a": 2}])
    assert not comparitor.__eq__(right)


# Note: is this a good way to do this?
def test_equal_via_method(spark_session: SparkSession):
    def data_frame_eq(self: DataFrame, other: DataFrame):
        self_data = [row.asDict() for row in self.collect()]
        other_data = [row.asDict() for row in self.collect()]
        return self_data == other_data

    DataFrame.__eq__ = data_frame_eq
    left: DataFrame = spark_session.createDataFrame([{"a": 1}])
    right: DataFrame = spark_session.createDataFrame([{"a": 1}])

    assert left == right
    assert left == [{"a": 1}]
