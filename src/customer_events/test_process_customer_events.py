from datetime import datetime
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)
from awsglue.context import GlueContext
from unittest.mock import patch, Mock, call, ANY
import pytest
from framework.test import (
    DataFrameMatcher,
    DynamicFrameMatcher,
    spark_context,
    mock_glue_context,
)
from customer_events.process_customer_events import (
    main,
    load_events,
    load_customers,
    save_customers,
)


@pytest.fixture
def customer_schema():
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("_as_of", TimestampType(), True),
        ]
    )


@pytest.fixture
def empty_customer_dataframe(
    mock_glue_context: GlueContext, customer_schema: StructType
):
    spark = mock_glue_context.spark_session
    return spark.createDataFrame(data=[], schema=customer_schema)


@patch("customer_events.process_customer_events.load_events")
@patch("customer_events.process_customer_events.load_customers")
@patch("customer_events.process_customer_events.save_customers")
def test_main_creates_new_customer(
    mock_save_customers: Mock,
    mock_load_customers: Mock,
    mock_load_events: Mock,
    mock_glue_context: GlueContext,
    empty_customer_dataframe: DataFrame,
):
    spark = mock_glue_context.spark_session
    mock_load_events.return_value = spark.createDataFrame(
        [
            {
                "payload": {
                    "before": None,
                    "after": {
                        "id": 1004,
                        "first_name": "Anne",
                        "last_name": "Kretchmar",
                        "email": "annek@noanswer.org",
                    },
                    "ts_ms": 1486500577691,
                }
            }
        ]
    )
    mock_load_customers.return_value = empty_customer_dataframe

    main(mock_glue_context)

    mock_load_events.assert_called_with(mock_glue_context)
    mock_load_customers.assert_called_with(mock_glue_context)
    mock_save_customers.assert_called_with(
        DataFrameMatcher(
            [
                {
                    "id": 1004,
                    "first_name": "Anne",
                    "last_name": "Kretchmar",
                    "email": "annek@noanswer.org",
                    "_as_of": datetime.fromisoformat("2017-02-07T20:49:37.691"),
                }
            ]
        ),
        mock_glue_context,
    )


@patch("customer_events.process_customer_events.load_events")
@patch("customer_events.process_customer_events.load_customers")
@patch("customer_events.process_customer_events.save_customers")
def test_main_updates_customer(
    mock_save_customers: Mock,
    mock_load_customers: Mock,
    mock_load_events: Mock,
    mock_glue_context: GlueContext,
):
    spark = mock_glue_context.spark_session
    mock_load_customers.return_value = spark.createDataFrame(
        [
            {
                "id": 1004,
                "first_name": "Anne",
                "last_name": "Kretchmar",
                "email": "annek@noanswer.org",
                "_as_of": datetime.fromisoformat("2017-02-07T20:49:37.691"),
            }
        ]
    )
    mock_load_events.return_value = spark.createDataFrame(
        [
            {
                "payload": {
                    "before": {},
                    "after": {
                        "id": 1004,
                        "first_name": "Anne",
                        "last_name": "Kretchmar",
                        "email": "new@noanswer.org",
                    },
                    "ts_ms": 1486500588691,
                }
            }
        ]
    )

    main(mock_glue_context)

    mock_load_events.assert_called_with(mock_glue_context)
    mock_load_customers.assert_called_with(mock_glue_context)
    mock_save_customers.assert_called_with(
        DataFrameMatcher(
            [
                {
                    "id": 1004,
                    "first_name": "Anne",
                    "last_name": "Kretchmar",
                    "email": "new@noanswer.org",
                    "_as_of": datetime.fromisoformat("2017-02-07T20:49:48.691"),
                }
            ]
        ),
        mock_glue_context,
    )


@patch("customer_events.process_customer_events.load_events")
@patch("customer_events.process_customer_events.load_customers")
@patch("customer_events.process_customer_events.save_customers")
def test_main_handles_new_customer_with_multiple_events(
    mock_save_customers: Mock,
    mock_load_customers: Mock,
    mock_load_events: Mock,
    mock_glue_context: GlueContext,
    empty_customer_dataframe: DataFrame,
):
    spark = mock_glue_context.spark_session
    mock_load_customers.return_value = empty_customer_dataframe
    mock_load_events.return_value = spark.createDataFrame(
        [
            {
                "payload": {
                    "before": {},
                    "after": {
                        "id": 1004,
                        "first_name": "Anne",
                        "last_name": "Kretchmar",
                        "email": "new@noanswer.org",
                    },
                    "ts_ms": 1486500588691,
                }
            },
            {
                "payload": {
                    "before": {},
                    "after": {
                        "id": 1004,
                        "first_name": "Anne",
                        "last_name": "Kretchmar",
                        "email": "new@noanswer.org",
                    },
                    "ts_ms": 1486500588691,
                }
            },
        ]
    )

    main(mock_glue_context)

    mock_load_events.assert_called_with(mock_glue_context)
    mock_load_customers.assert_called_with(mock_glue_context)
    mock_save_customers.assert_called_with(
        DataFrameMatcher(
            [
                {
                    "id": 1004,
                    "first_name": "Anne",
                    "last_name": "Kretchmar",
                    "email": "new@noanswer.org",
                    "_as_of": datetime.fromisoformat("2017-02-07T20:49:48.691"),
                }
            ]
        ),
        mock_glue_context,
    )


@patch("customer_events.process_customer_events.load_events")
@patch("customer_events.process_customer_events.load_customers")
@patch("customer_events.process_customer_events.save_customers")
def test_main_removes_deleted_customer(
    mock_save_customers: Mock,
    mock_load_customers: Mock,
    mock_load_events: Mock,
    mock_glue_context: GlueContext,
):
    spark = mock_glue_context.spark_session
    mock_load_customers.return_value = spark.createDataFrame(
        [
            {
                "id": 1004,
                "first_name": "Anne",
                "last_name": "Kretchmar",
                "email": "annek@noanswer.org",
                "_as_of": datetime.fromisoformat("2017-02-07T20:49:37.691"),
            }
        ]
    )
    mock_load_events.return_value = spark.createDataFrame(
        [
            {
                "payload": {
                    "before": {
                        "id": 1004,
                        "first_name": "Anne",
                        "last_name": "Kretchmar",
                        "email": "new@noanswer.org",
                    },
                    "after": None,
                    "ts_ms": 1486500588691,
                }
            }
        ]
    )

    main(mock_glue_context)

    mock_load_events.assert_called_with(mock_glue_context)
    mock_load_customers.assert_called_with(mock_glue_context)
    mock_save_customers.assert_called_with(
        DataFrameMatcher([]),
        mock_glue_context,
    )
