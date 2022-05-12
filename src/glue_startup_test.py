# library imports
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.context import SparkContext

# application imports
from glue_startup import glue_dynamic_frame_crossjoin_demo, glue_data_frame_demo

# spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

gc = GlueContext(sc)

data1 = [{"a": 1, "b": 2}]
data2 = [{"c": 2, "d": 1}, {"c": 1, "d": 2}]

def test_glue_dynamic_frame_crossjoin_demo_returns_expected_results():
    dyf1 = DynamicFrame.fromDF(spark.createDataFrame(data1), gc, "glue_dyf1")
    dyf2 = DynamicFrame.fromDF(spark.createDataFrame(data2), gc, "glue_dyf2")

    result = glue_dynamic_frame_crossjoin_demo(dyf1, dyf2)
    assert [row.asDict() for row in result.toDF().collect()] == [{"a": 1, "b": 2, "c": 2, "d": 1}, {"c": 1, "d": 2, "a": 1, "b": 2}]

def test_glue_data_frame_demo_returns_results():
    dyf1 = DynamicFrame.fromDF(spark.createDataFrame(data1), gc, "glue_dyf1")
    dyf2 = DynamicFrame.fromDF(spark.createDataFrame(data2), gc, "glue_dyf2")

    result = glue_data_frame_demo(dyf1, dyf2)
    assert [row.asDict() for row in result.toDF().collect()] == [{"a": 1, "b": 2, "c": 1, "d": 2}]


