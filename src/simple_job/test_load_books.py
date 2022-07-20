from datetime import date
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from unittest.mock import patch, Mock, call, ANY
from framework.fixtures import spark_context, mock_glue_context
from simple_job.load_books import main, load_books, save_books


@patch("simple_job.load_books.get_job_arguments")
def test_load_books(mock_get_job_arguments: Mock, mock_glue_context: GlueContext):
    mock_get_job_arguments.return_value = ("mock_bucket",)
    mock_data = mock_glue_context.create_dynamic_frame_from_rdd(
        mock_glue_context.spark_session.sparkContext.parallelize([{"a": 1}]),
        "sample input",
    )
    mock_glue_context.create_dynamic_frame_from_options.return_value = mock_data

    actualDF = load_books(mock_glue_context)

    mock_get_job_arguments.assert_called_with("source_bucket")
    mock_glue_context.create_dynamic_frame_from_options.assert_called_with(
        connection_type="s3",
        connection_options={"paths": ["s3://mock_bucket/sample_data/json/books"]},
        format="json",
    )
    expectedData = [{"a": 1}]
    assert type(actualDF) is DataFrame
    assert [row.asDict() for row in actualDF.collect()] == expectedData


@patch("simple_job.load_books.load_books")
@patch("simple_job.load_books.save_books")
def test_main_converts_books(
    mock_save_books: Mock, mock_load_books: Mock, mock_glue_context: GlueContext
):
    book_df = mock_glue_context.spark_session.createDataFrame(
        [{"title": "t", "publish_date": "2022-02-04", "author": "a"}]
    )
    mock_load_books.return_value = book_df

    main(mock_glue_context)

    mock_load_books.assert_called_with(mock_glue_context)
    mock_save_books.assert_called_with(
        EqualDataFrame(
            [
                {
                    "title": "t",
                    "publish_date": date.fromisoformat("2022-02-04"),
                    "author_name": "a",
                }
            ]
        ),
        mock_glue_context,
    )


def test_save_books(mock_glue_context: GlueContext):
    call_order_mock = Mock()
    call_order_mock.purge_table = mock_glue_context.purge_table
    call_order_mock.write_dynamic_frame_from_catalog = mock_glue_context.write_dynamic_frame_from_catalog
    book_df = mock_glue_context.spark_session.createDataFrame(
        [
            {
                "title": "t",
                "publish_date": date.fromisoformat("2022-02-04"),
                "author_name": "a",
            }
        ]
    )

    save_books(book_df, mock_glue_context)

    mock_glue_context.purge_table.assert_called_with("glue_reference", "raw_books", options={"retentionPeriod": 0})
    mock_glue_context.write_dynamic_frame_from_catalog.assert_called_with(
        EqualDynamicFrame(
            [
                {
                    "title": "t",
                    "publish_date": date.fromisoformat("2022-02-04"),
                    "author_name": "a",
                }
            ]
        ),
        "glue_reference",
        "raw_books",
    )
    call_order_mock.assert_has_calls([
        call.purge_table(ANY, ANY),
        call.write_dynamic_frame_from_catalog(ANY, ANY, ANY)
    ])


class EqualDataFrame(DataFrame):
    expected = []

    def __init__(self, expected: List[Dict]):
        self.expected = expected

    def __repr__(self):
        return f"Expected: {repr(self.expected)}"

    def __eq__(self, other: DynamicFrame):
        other_rows = [row.asDict() for row in other.collect()]
        return other_rows == self.expected


class EqualDynamicFrame(DynamicFrame):
    expected = []

    def __init__(self, expected: List[Dict]):
        self.expected = expected

    def __repr__(self):
        return f"Expected: {repr(self.expected)}"

    def __eq__(self, other: DynamicFrame):
        other_rows = [row.asDict() for row in other.toDF().collect()]
        return other_rows == self.expected
