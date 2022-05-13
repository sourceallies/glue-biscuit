# library imports
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.context import SparkContext
from unittest.mock import patch

# application imports
from glue_api_example import get_data

# spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# glue
gc = GlueContext(sc)

@patch.object(GlueContext, 'create_dynamic_frame_from_options')
def test_get_data_and_return_it(mock_method):
    expected_dynamic_frame = DynamicFrame.fromDF(spark.createDataFrame([{"a": 1, "b": 2}]), gc, "expected_dynamic_frame")
    
    mock_method.return_value = expected_dynamic_frame

    actual_dynamic_frame = get_data()

    assert actual_dynamic_frame == expected_dynamic_frame
    mock_method.assert_called_with(
        connection_type="s3",
        connection_options={"paths": ["S3://oedi-data-lake/pv-rooftop/buildings/city_year=minneapolis_mn_07/part-00023-647f1a88-3404-4ce7-9c7f-f9e18fcfb3be.c000.snappy.parquet"]},
        format="parquet"
    )
