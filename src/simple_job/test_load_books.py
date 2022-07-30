from datetime import date
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from unittest.mock import patch, Mock, call, ANY
from framework import (
    DataFrameMatcher,
    DynamicFrameMatcher,
    spark_context,
    mock_glue_context,
)
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
        DataFrameMatcher(
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

    mock_glue_context.purge_table.assert_called_with(
        "glue_reference", "raw_books", options={"retentionPeriod": 0}
    )
    mock_glue_context.write_dynamic_frame_from_catalog.assert_called_with(
        DynamicFrameMatcher(
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
    purge_table_index = mock_glue_context.mock_calls.index(
        call.purge_table(ANY, ANY, options=ANY)
    )
    write_index = mock_glue_context.mock_calls.index(
        call.write_dynamic_frame_from_catalog(ANY, ANY, ANY)
    )
    assert purge_table_index < write_index
