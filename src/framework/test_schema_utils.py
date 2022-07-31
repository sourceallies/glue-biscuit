import pytest

from framework.schema_utils import coerce_to_schema
from framework import DataFrameMatcher
from pyspark.sql import DataFrame, Row
from pyspark.sql.context import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, \
    ArrayType, StringType


@pytest.fixture
def test_df(spark_session: SparkSession) -> DataFrame:
    yield spark_session.createDataFrame(
        [{'x': 1}, {'x': 2}],
        StructType([
            StructField('x', IntegerType())
        ]))


def test_coerces_single_column(test_df: DataFrame):
    result = coerce_to_schema(
        test_df,
        StructType([
            StructField('x', LongType())
        ])
    )

    assert isinstance(result.schema['x'].dataType, LongType)
    assert result == DataFrameMatcher([
        {'x': 1},
        {'x': 2}
    ])


def test_removes_extra_columns(test_df: DataFrame):
    test_df = test_df.withColumn('y', test_df['x'])
    result = coerce_to_schema(
        test_df,
        StructType([
            StructField('y', LongType())
        ])
    )

    assert result.schema.fieldNames() == ['y']
    assert result == DataFrameMatcher([
        {'y': 1},
        {'y': 2}
    ])


def test_coerces_struct_types(spark_session: SparkSession):
    df = spark_session.createDataFrame([
        {
            'struct_field': {
                'value_field': 42
            }
        }
    ], schema=StructType([
        StructField('struct_field', StructType([
            StructField('value_field', IntegerType())
        ]))
    ]))

    result = coerce_to_schema(df, StructType([
        StructField('struct_field', StructType([
            StructField('value_field', LongType())
        ]))
    ]))

    struct_type: StructType = result.schema['struct_field'].dataType
    value_type = struct_type['value_field'].dataType

    result.show(truncate=False)
    assert isinstance(value_type, LongType)
    assert result == DataFrameMatcher([
        {
            'struct_field': Row(value_field=42)
        }
    ])


def test_coerces_array_types(spark_session: SparkSession):
    df = spark_session.createDataFrame([
        {'x': [1, 2, 3, 4]}
    ],
        schema=StructType([
            StructField('x', ArrayType(IntegerType()))
        ])
    )
    result = coerce_to_schema(
        df,
        StructType([
            StructField('x', ArrayType(LongType()))
        ])
    )

    array_field: ArrayType = result.schema['x'].dataType
    assert isinstance(array_field.elementType, LongType)
    assert result == DataFrameMatcher([
        {'x': [1, 2, 3, 4]}
    ])


def test_coerces_binary_incompatible_types(test_df: DataFrame):
    result = coerce_to_schema(
        test_df,
        StructType([
            StructField('x', StringType())
        ])
    )

    assert isinstance(result.schema['x'].dataType, StringType)
    assert result == DataFrameMatcher([
        {'x': '1'},
        {'x': '2'},
    ])
