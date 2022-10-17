from datetime import datetime
import pytest
from unittest.mock import patch, Mock
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from framework.test import DataFrameMatcher
from pyspark import SparkContext
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    BooleanType,
    StringType,
)
from framework.merge_utils import create_merge_function


@pytest.fixture
def record_schema():
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), False),
            StructField("_as_of", TimestampType(), False),
            StructField("_deleted", BooleanType(), False),
        ]
    )


def test_merge_empties(spark_session: SparkSession, record_schema: StructType):
    a = spark_session.createDataFrame([], record_schema)
    b = spark_session.createDataFrame([], record_schema)
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(a, b)

    assert 0 == merged.count()


def test_merge_single_to_empty(spark_session: SparkSession, record_schema: StructType):
    record = {
        "id": 1,
        "value": "a",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:00"),
        "_deleted": False,
    }
    a = spark_session.createDataFrame([record], record_schema)
    b = spark_session.createDataFrame([], record_schema)
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(a, b)

    assert DataFrameMatcher([record]) == merged


def test_empty_to_single(spark_session: SparkSession, record_schema: StructType):
    record = {
        "id": 1,
        "value": "a",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:00"),
        "_deleted": False,
    }
    a = spark_session.createDataFrame([], record_schema)
    b = spark_session.createDataFrame([record], record_schema)
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(a, b)

    assert merged == DataFrameMatcher([record])


def test_same_record_in_both_sets(
    spark_session: SparkSession, record_schema: StructType
):
    record = {
        "id": 1,
        "value": "a",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:00"),
        "_deleted": False,
    }
    a = spark_session.createDataFrame([record], record_schema)
    b = spark_session.createDataFrame([record], record_schema)
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(a, b)

    assert merged == DataFrameMatcher([record])


def test_record_overwritten(spark_session: SparkSession, record_schema: StructType):
    record_a = {
        "id": 1,
        "value": "b",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:01"),
        "_deleted": False,
    }
    record_b = {
        "id": 1,
        "value": "b",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:00"),
        "_deleted": False,
    }
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(
        spark_session.createDataFrame([record_a], record_schema),
        spark_session.createDataFrame([record_b], record_schema),
    )

    assert merged == DataFrameMatcher([record_a])


def test_old_record_passed_in(spark_session: SparkSession, record_schema: StructType):
    record_a = {
        "id": 1,
        "value": "a",
        "_as_of": datetime.fromisoformat("2020-01-01T09:40:00"),
        "_deleted": False,
    }
    record_b = {
        "id": 1,
        "value": "b",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:00"),
        "_deleted": False,
    }
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(
        spark_session.createDataFrame([record_a], record_schema),
        spark_session.createDataFrame([record_b], record_schema),
    )

    assert merged == DataFrameMatcher([record_b])


def test_record_deleted(spark_session: SparkSession, record_schema: StructType):
    record_a = {
        "id": 1,
        "value": "b",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:01"),
        "_deleted": True,
    }
    record_b = {
        "id": 1,
        "value": "b",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:00"),
        "_deleted": False,
    }
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(
        spark_session.createDataFrame([record_a], record_schema),
        spark_session.createDataFrame([record_b], record_schema),
    )

    assert merged.count() == 0


def test_correct_record_is_updated(
    spark_session: SparkSession, record_schema: StructType
):
    record_a1 = {
        "id": 1,
        "value": "a",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:01"),
        "_deleted": False,
    }
    record_a2 = {
        "id": 2,
        "value": "a",
        "_as_of": datetime.fromisoformat("2020-01-01T10:00:01"),
        "_deleted": False,
    }
    record_b = {
        "id": 1,
        "value": "b",
        "_as_of": datetime.fromisoformat("2020-01-01T10:01:00"),
        "_deleted": False,
    }
    merge_function = create_merge_function(
        key_fields=["id"], sort_field="_as_of", deleted_field="_deleted"
    )
    merged = merge_function(
        spark_session.createDataFrame([record_b], record_schema),
        spark_session.createDataFrame([record_a1, record_a2], record_schema),
    )
    assert merged == DataFrameMatcher([record_b, record_a2])
