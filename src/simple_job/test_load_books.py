from awsglue.context import GlueContext, DynamicFrame
from unittest.mock import patch, Mock
import pytest
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from load_books import main, load_books, save_books




@pytest.fixture()
def mock_glue_context():
    spark = SparkSession.builder.getOrCreate()
    gc = GlueContext(spark.sparkContext)

    val = gc.create_dynamic_frame_from_rdd(
        spark.sparkContext.parallelize([
            {"a": 1}
        ]),
        'sample input'
    )

    gc.create_dynamic_frame_from_options = Mock(
        "create_dynamic_frame_from_options",
        return_value=val
    )
    yield gc


def test_load_books(mock_glue_context: GlueContext):
    actualDF = load_books(mock_glue_context)

    mock_glue_context.create_dynamic_frame_from_options.assert_called_with(
        connection_type="s3",
        connection_options={"paths": ["s3://glue-reference-implementation-databucket-fed75mq4rmq0/sample_data/json/books"]},
        format="json",
    )
    expectedData = [{"a": 1}]
    assert type(actualDF) is DataFrame
    assert [row.asDict() for row in actualDF.collect()] == expectedData
