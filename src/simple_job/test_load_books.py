from awsglue.context import GlueContext
from unittest.mock import patch, Mock
import pytest
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame

from load_books import main, load_books, save_books

@pytest.fixture()
def mock_glue_context():
    gc = GlueContext(SparkContext.getOrCreate())
    gc.create_dynamic_frame_from_options = Mock('create_dynamic_frame_from_options',
        return_value=DynamicFrame.
    )
    yield gc

def test_load_books(mock_glue_context: GlueContext):
    mock_result = DataFrame()
    gc.create_dynamic_frame_from_options.return_value = mock_result
    actual_result = load_books(test_load_books)

    assert mock_glue_context.create_dynamic_frame_from_options.assert_called_with(
        connection_type="s3",
        connection_options={
            "paths": ["s3://dummy_bucket/sample_data/json/books"]
        },
        format="parquet"
    )
    assert actual_result == mock_result
