import pytest
from pyspark.sql import SparkSession, DataFrame
from awsglue.context import GlueContext
from unittest.mock import patch, Mock


@pytest.fixture(scope="session")
def spark_context():
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    yield spark.sparkContext


@pytest.fixture
def mock_glue_context(spark_context):
    mocked_methods = [
        "create_dynamic_frame_from_catalog",
        "create_dynamic_frame_from_options",
        "create_sample_dynamic_frame_from_catalog",
        "create_sample_dynamic_frame_from_options",
        "getSource",
        "create_data_frame_from_catalog",
        "create_data_frame_from_options",
        "forEachBatch",
        "getSink",
        "write_dynamic_frame_from_options",
        "write_from_options",
        "write_dynamic_frame_from_catalog",
        "write_dynamic_frame_from_jdbc_conf",
        "write_from_jdbc_conf",
        "purge_table",
        "purge_s3_path",
    ]
    gc = GlueContext(spark_context)
    context_mock = Mock(spec=gc, wraps=gc)

    for name in mocked_methods:
        method_mock: Mock = getattr(context_mock, name, None)
        if method_mock is not None:
            method_mock.return_value = None

    yield context_mock
