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
    gc = GlueContext(spark_context)

    gc.create_dynamic_frame_from_catalog = Mock("create_dynamic_frame_from_catalog")
    gc.create_dynamic_frame_from_options = Mock("create_dynamic_frame_from_options")
    gc.create_sample_dynamic_frame_from_catalog = Mock(
        "create_sample_dynamic_frame_from_catalog"
    )
    gc.create_sample_dynamic_frame_from_options = Mock(
        "create_sample_dynamic_frame_from_options"
    )
    gc.getSource = Mock("getSource")
    gc.create_data_frame_from_catalog = Mock("create_data_frame_from_catalog")
    gc.create_data_frame_from_options = Mock("create_data_frame_from_options")
    gc.forEachBatch = Mock("forEachBatch")

    gc.getSink = Mock("getSink")
    gc.write_dynamic_frame_from_options = Mock("write_dynamic_frame_from_options")
    gc.write_from_options = Mock("write_from_options")
    gc.write_dynamic_frame_from_catalog = Mock("write_dynamic_frame_from_catalog")
    gc.write_dynamic_frame_from_jdbc_conf = Mock("write_dynamic_frame_from_jdbc_conf")
    gc.write_from_jdbc_conf = Mock("write_from_jdbc_conf")

    gc.purge_table = Mock("purge_table")
    gc.purge_s3_path = Mock("purge_s3_path")

    yield gc
