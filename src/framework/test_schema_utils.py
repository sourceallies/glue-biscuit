import pytest
from unittest.mock import patch, Mock
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from framework.schema_utils import coerce_to_schema, source, schema
from framework.test import DataFrameMatcher
from pyspark import SparkContext
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    ArrayType,
    StringType,
)


@pytest.fixture
def test_df(spark_session: SparkSession) -> DataFrame:
    yield spark_session.createDataFrame(
        [{"x": 1}, {"x": 2}], StructType([StructField("x", IntegerType())])
    )


@pytest.fixture
def test_dyf(test_df: DataFrame, spark_context: SparkContext) -> DataFrame:
    return DynamicFrame.fromDF(test_df, GlueContext(spark_context), "test_dyf")


def test_coerces_single_column(test_df: DataFrame):
    result = coerce_to_schema(test_df, StructType([StructField("x", LongType())]))

    assert isinstance(result.schema["x"].dataType, LongType)
    assert result == DataFrameMatcher([{"x": 1}, {"x": 2}])


def test_removes_extra_columns(test_df: DataFrame):
    test_df = test_df.withColumn("y", test_df["x"])
    result = coerce_to_schema(test_df, StructType([StructField("y", LongType())]))

    assert result.schema.fieldNames() == ["y"]
    assert result == DataFrameMatcher([{"y": 1}, {"y": 2}])


def test_coerces_struct_types(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [{"struct_field": {"nested_struct_field": {"value_field": 42}}}],
        schema=StructType(
            [
                StructField(
                    "struct_field",
                    StructType(
                        [
                            StructField(
                                "nested_struct_field",
                                StructType([StructField("value_field", IntegerType())]),
                            )
                        ]
                    ),
                )
            ]
        ),
    )

    result = coerce_to_schema(
        df,
        StructType(
            [
                StructField(
                    "struct_field",
                    StructType(
                        [
                            StructField(
                                "nested_struct_field",
                                StructType([StructField("value_field", LongType())]),
                            )
                        ]
                    ),
                )
            ]
        ),
    )

    struct_type: StructType = result.schema["struct_field"].dataType
    nested_struct_type: StructType = struct_type["nested_struct_field"].dataType
    value_type = nested_struct_type["value_field"].dataType

    result.show(truncate=False)
    assert isinstance(value_type, LongType)
    assert result == DataFrameMatcher(
        [{"struct_field": Row(nested_struct_field=Row(value_field=42))}]
    )


def test_coerces_array_types(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [{"x": [1, 2, 3, 4]}],
        schema=StructType([StructField("x", ArrayType(IntegerType()))]),
    )
    result = coerce_to_schema(df, StructType([StructField("x", ArrayType(LongType()))]))

    array_field: ArrayType = result.schema["x"].dataType
    assert isinstance(array_field.elementType, LongType)
    assert result == DataFrameMatcher([{"x": [1, 2, 3, 4]}])


def test_coerces_binary_incompatible_types(test_df: DataFrame):
    result = coerce_to_schema(test_df, StructType([StructField("x", StringType())]))

    assert isinstance(result.schema["x"].dataType, StringType)
    assert result == DataFrameMatcher(
        [
            {"x": "1"},
            {"x": "2"},
        ]
    )


@schema(schema_obj=StructType([StructField("x", LongType())]))
def test_schema_coerces_data_frame_from_schema_object(test_df):
    assert isinstance(test_df.schema["x"].dataType, LongType)
    assert test_df == DataFrameMatcher([{"x": 1}, {"x": 2}])


@schema(schema_func=lambda: StructType([StructField("x", LongType())]))
def test_schema_coerces_data_frame_from_schema_function(test_df):
    assert isinstance(test_df.schema["x"].dataType, LongType)
    assert test_df == DataFrameMatcher([{"x": 1}, {"x": 2}])


@patch("framework.schema_utils.GlueContext")
def test_source_gets_table_from_glue(mock_glue_context_class: Mock):
    mock_glue_context = Mock(name="GlueContext")
    mock_glue_context_class.return_value = mock_glue_context

    @source(
        "some_db", "some_table", schema_obj=StructType([StructField("x", LongType())])
    )
    def test_func(frame):
        pass

    test_func()

    mock_glue_context.assert_called_once_with(
        database="some_db",
        table_name="some_table",
        transformation_ctx="source-some_db-some_table",
    )


@patch("framework.schema_utils.GlueContext")
def test_source_gets_table_from_glue(
    mock_glue_context_class: Mock, test_dyf: DynamicFrame
):
    mock_glue_context: DynamicFrame = Mock(name="GlueContext")
    mock_glue_context.create_dynamic_frame_from_catalog.return_value = test_dyf
    mock_glue_context_class.return_value = mock_glue_context

    @source(
        "some_db", "some_table", schema_obj=StructType([StructField("x", LongType())])
    )
    def test_func(frame):
        return frame

    result = test_func()

    assert isinstance(result.schema["x"].dataType, LongType)
    assert result == DataFrameMatcher([{"x": 1}, {"x": 2}])
