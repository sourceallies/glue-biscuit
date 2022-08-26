import pytest

from framework.schema_utils import coerce_to_schema, schema_from_cloudformation
from framework.test import DataFrameMatcher
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    ArrayType,
    StringType,
    ByteType,
    ShortType,
    DateType,
    TimestampType,
    DecimalType,
)


@pytest.fixture
def test_df(spark_session: SparkSession) -> DataFrame:
    yield spark_session.createDataFrame(
        [{"x": 1}, {"x": 2}], StructType([StructField("x", IntegerType())])
    )


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


def test_schema_from_cloudformation_parses_string():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "title"
    ][0]
    assert isinstance(field_under_test.dataType, StringType)


def test_schema_from_cloudformation_parses_tinyint():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "author_age"
    ][0]
    assert isinstance(field_under_test.dataType, ByteType)


def test_schema_from_cloudformation_parses_smallint():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "puddles"
    ][0]
    assert isinstance(field_under_test.dataType, ShortType)


def test_schema_from_cloudformation_parses_int():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "kangaroos_in_australia"
    ][0]
    assert isinstance(field_under_test.dataType, IntegerType)


def test_schema_from_cloudformation_parses_long():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "trees_in_world"
    ][0]
    assert isinstance(field_under_test.dataType, LongType)


def test_schema_from_cloudformation_parses_date():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "publish_date"
    ][0]
    assert isinstance(field_under_test.dataType, DateType)


def test_schema_from_cloudformation_parses_timestamp():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "author_time"
    ][0]
    assert isinstance(field_under_test.dataType, TimestampType)


def test_schema_from_cloudformation_parses_decimal():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "royalties_owed"
    ][0]
    data_type: DecimalType = field_under_test.dataType
    assert isinstance(data_type, DecimalType)
    assert data_type.precision == 38
    assert data_type.scale == 2


def test_schema_from_cloudformation_parses_struct():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [field for field in fields if field.name == "pet"][
        0
    ]
    data_type: StructType = field_under_test.dataType
    assert isinstance(data_type, StructType)
    assert data_type.fields == [
        StructField("name", StringType()),
        StructField("age", IntegerType()),
    ]


def test_schema_from_cloudformation_parses_array():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "stats"
    ][0]
    data_type: ArrayType = field_under_test.dataType
    assert isinstance(data_type, ArrayType)
    assert isinstance(data_type.elementType, IntegerType)


def test_schema_from_cloudformation_parses_array_of_structs():
    schema = schema_from_cloudformation("./test_template.yml", "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "metadata"
    ][0]

    data_type: ArrayType = field_under_test.dataType
    internal_struct: StructType = data_type.elementType

    assert isinstance(data_type, ArrayType)
    assert isinstance(internal_struct, StructType)
    assert internal_struct.fields == [
        StructField("key", StringType()),
        StructField("value", StringType()),
    ]
