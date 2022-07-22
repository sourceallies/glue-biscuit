from pyspark import SparkContext
import pytest
from pyspark.sql import SparkSession, SparkSession, DataFrame
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from framework.fixtures import mock_glue_context, spark_context
from framework.dynamic_frame_matcher import DynamicFrameMatcher

@pytest.fixture()
def right_frame(mock_glue_context: GlueContext, spark_context: SparkContext) -> DynamicFrame:
    return mock_glue_context.create_dynamic_frame_from_rdd(
        spark_context.parallelize([{"a": 1}]),
        "sample input"
    )
    

def test_equal_when_one_row_column(right_frame: DynamicFrame):
    matcher = DynamicFrameMatcher([{"a": 1}])
    assert matcher.__eq__(right_frame)


def test_not_equal_when_column_names_differ(right_frame: DynamicFrame):
    matcher = DynamicFrameMatcher([{"b": 1}])
    assert not matcher.__eq__(right_frame)


def test_not_equal_when_column_value_differs(right_frame: DynamicFrame):
    matcher = DynamicFrameMatcher([{"a": 2}])
    assert not matcher.__eq__(right_frame)
