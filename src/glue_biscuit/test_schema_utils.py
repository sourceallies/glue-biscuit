import os
import pytest
from glue_biscuit.schema_utils import (
    coerce_to_schema,
    schema_from_cloudformation,
    schema_from_glue,
    source,
    schema,
    sink,
)
from unittest.mock import patch, Mock
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from glue_biscuit.test import DataFrameMatcher, DynamicFrameMatcher
from pyspark import SparkContext
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

test_template_path = "./test_data/test_template.yml"


@pytest.fixture
def test_df(spark_session: SparkSession) -> DataFrame:
    yield spark_session.createDataFrame(
        [{"x": 1}, {"x": 2}], StructType([StructField("x", IntegerType())])
    )


@pytest.fixture
def test_dyf(test_df: DataFrame, spark_context: SparkContext) -> DataFrame:
    return DynamicFrame.fromDF(test_df, GlueContext(spark_context), "test_dyf")


@pytest.fixture(autouse=True)
def mock_environ():
    old_region = os.environ.get("AWS_REGION", "")
    os.environ["AWS_REGION"] = "us-north-5"
    yield
    os.environ["AWS_REGION"] = old_region


@pytest.fixture
def mock_glue_context(test_dyf: DynamicFrame, spark_context: SparkContext):
    real_context = GlueContext(spark_context)
    mock_context = Mock(name="GlueContext", spec=GlueContext)
    mock_context.create_dynamic_frame_from_catalog.return_value = test_dyf
    mock_context._jvm = real_context._jvm
    mock_context._ssql_ctx = real_context._ssql_ctx
    mock_context._sc = real_context._sc
    return mock_context


@pytest.fixture
def glue_response():
    return {
        "Table": {
            "Name": "books",
            "DatabaseName": "glue_reference",
            "Description": "Books data product",
            "CreateTime": "2022-08-11T21:55:34-05:00",
            "UpdateTime": "2022-08-21T20:05:54-05:00",
            "Retention": 0,
            "StorageDescriptor": {
                "Columns": [
                    {
                        "Name": "title",
                        "Type": "string",
                        "Comment": "Full title of this book",
                    },
                    {
                        "Name": "publish_date",
                        "Type": "date",
                        "Comment": "The date this book was first published",
                    },
                    {
                        "Name": "author_name",
                        "Type": "string",
                        "Comment": "The full name of the author",
                    },
                    {
                        "Name": "author_birth_date",
                        "Type": "date",
                        "Comment": "The date the author was born",
                    },
                    {
                        "Name": "author_id",
                        "Type": "bigint",
                        "Comment": "Unique identifier for this author",
                    },
                ],
                "Location": "s3://glue-reference-implementation-databucket-fed75mq4rmq0/books",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": 0,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
                "SortColumns": [],
                "Parameters": {"classification": "parquet"},
                "StoredAsSubDirectories": False,
            },
            "CreatedBy": "arn:aws:sts::144406111952:assumed-role/CognitoSAI-FederatedUserRole-3IHKNHJY8G3F/CognitoIdentityCredentials",
            "IsRegisteredWithLakeFormation": False,
            "CatalogId": "144406111952",
            "VersionId": "2",
        }
    }


@pytest.fixture
def mock_glue_client(glue_response):
    glue = Mock(name="glue_client")
    glue.get_table.return_value = glue_response
    yield glue


@pytest.fixture(autouse=True)
def mock_boto3(mock_glue_client):
    with patch("glue_biscuit.schema_utils.boto3") as boto:
        boto.client.side_effect = lambda *args, **kwargs: {"glue": mock_glue_client}[
            args[0]
        ]
        yield boto


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
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "title"
    ][0]
    assert isinstance(field_under_test.dataType, StringType)


def test_schema_from_cloudformation_parses_tinyint():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "author_age"
    ][0]
    assert isinstance(field_under_test.dataType, ByteType)


def test_schema_from_cloudformation_parses_smallint():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "puddles"
    ][0]
    assert isinstance(field_under_test.dataType, ShortType)


def test_schema_from_cloudformation_parses_int():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "kangaroos_in_australia"
    ][0]
    assert isinstance(field_under_test.dataType, IntegerType)


def test_schema_from_cloudformation_parses_long():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "trees_in_world"
    ][0]
    assert isinstance(field_under_test.dataType, LongType)


def test_schema_from_cloudformation_parses_date():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "publish_date"
    ][0]
    assert isinstance(field_under_test.dataType, DateType)


def test_schema_from_cloudformation_parses_timestamp():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "author_time"
    ][0]
    assert isinstance(field_under_test.dataType, TimestampType)


def test_schema_from_cloudformation_parses_decimal():
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "royalties_owed"
    ][0]
    data_type: DecimalType = field_under_test.dataType
    assert isinstance(data_type, DecimalType)
    assert data_type.precision == 38
    assert data_type.scale == 2


def test_schema_from_cloudformation_parses_struct():
    schema = schema_from_cloudformation(test_template_path, "test_table")
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
    schema = schema_from_cloudformation(test_template_path, "test_table")
    fields = schema.fields
    field_under_test: StructField = [
        field for field in fields if field.name == "stats"
    ][0]
    data_type: ArrayType = field_under_test.dataType
    assert isinstance(data_type, ArrayType)
    assert isinstance(data_type.elementType, IntegerType)


def test_schema_from_cloudformation_parses_array_of_structs():
    schema = schema_from_cloudformation(test_template_path, "test_table")
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


def test_schema_from_glue_parses_glue_response():
    res = schema_from_glue("some_db", "some_table")
    assert res == StructType(
        [
            StructField("title", StringType()),
            StructField("publish_date", DateType()),
            StructField("author_name", StringType()),
            StructField("author_birth_date", DateType()),
            StructField("author_id", LongType()),
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


def test_source_gets_table_from_glue(mock_glue_context: Mock):
    @source(
        "some_db", "some_table", schema_obj=StructType([StructField("x", LongType())])
    )
    def test_func(frame, glue_context):
        pass

    test_func(mock_glue_context)

    mock_glue_context.create_dynamic_frame_from_catalog.assert_called_once_with(
        database="some_db",
        table_name="some_table",
        transformation_ctx="source-some_db-some_table",
    )


def test_source_coerces_schema(mock_glue_context: Mock):
    @source(
        "some_db", "some_table", schema_obj=StructType([StructField("x", LongType())])
    )
    def test_func(frame, glue_context):
        return frame

    result = test_func(mock_glue_context)

    assert isinstance(result.schema["x"].dataType, LongType)
    assert result == DataFrameMatcher([{"x": 1}, {"x": 2}])


def test_sink_writes_table_to_glue(mock_glue_context: Mock, test_dyf: DynamicFrame):
    @sink(
        "some_db", "some_table", schema_obj=StructType([StructField("x", LongType())])
    )
    def test_func(mock_glue_context):
        return test_dyf.toDF()

    test_func(mock_glue_context)

    mock_glue_context.write_dynamic_frame_from_catalog.assert_called_once_with(
        DynamicFrameMatcher([{"x": 1}, {"x": 2}]), "some_db", "some_table"
    )


def test_sink_coerces_schema(mock_glue_context: Mock, test_dyf: DynamicFrame):
    @sink(
        "some_db", "some_table", schema_obj=StructType([StructField("x", StringType())])
    )
    def test_func(mock_glue_context):
        return test_dyf.toDF()

    test_func(mock_glue_context)

    mock_glue_context.write_dynamic_frame_from_catalog.assert_called_once_with(
        DynamicFrameMatcher([{"x": "1"}, {"x": "2"}]), "some_db", "some_table"
    )
