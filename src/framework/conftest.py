import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_context():
    # TODO: move me when the fixtures is merged
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    yield spark.sparkContext


@pytest.fixture()
def spark_session(spark_context: SparkContext):
    yield SparkSession(spark_context)
