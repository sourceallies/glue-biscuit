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
from books_data_product.load_books import main, load_books, load_authors, save_books


@patch("books_data_product.load_books.get_job_arguments")
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


@patch("books_data_product.load_books.get_job_arguments")
def test_load_authors(mock_get_job_arguments: Mock, mock_glue_context: GlueContext):
    mock_get_job_arguments.return_value = ("mock_bucket",)
    mock_data = mock_glue_context.create_dynamic_frame_from_rdd(
        mock_glue_context.spark_session.sparkContext.parallelize([{"a": 1}]),
        "sample input",
    )
    mock_glue_context.create_dynamic_frame_from_options.return_value = mock_data

    actualDF = load_authors(mock_glue_context)

    mock_get_job_arguments.assert_called_with("source_bucket")
    mock_glue_context.create_dynamic_frame_from_options.assert_called_with(
        connection_type="s3",
        connection_options={"paths": ["s3://mock_bucket/sample_data/json/authors"]},
        format="json",
    )
    expectedData = [{"a": 1}]
    assert type(actualDF) is DataFrame
    assert [row.asDict() for row in actualDF.collect()] == expectedData


@patch("books_data_product.load_books.load_books")
@patch("books_data_product.load_books.load_authors")
@patch("books_data_product.load_books.save_books")
def test_main_joins_and_writes_a_row(
    mock_save_books: Mock,
    mock_load_authors: Mock,
    mock_load_books: Mock,
    mock_glue_context: GlueContext,
):
    spark = mock_glue_context.spark_session
    mock_load_books.return_value = spark.createDataFrame(
        [{"title": "t", "publish_date": "2022-02-04", "author": "a"}]
    )
    mock_load_authors.return_value = spark.createDataFrame(
        [{"name": "a", "id": 34, "birth_date": "1994-04-03"}]
    )

    main(mock_glue_context)

    mock_load_books.assert_called_with(mock_glue_context)
    mock_load_authors.assert_called_with(mock_glue_context)
    mock_save_books.assert_called_with(
        DataFrameMatcher(
            [
                {
                    "title": "t",
                    "publish_date": date.fromisoformat("2022-02-04"),
                    "author_name": "a",
                    "author_birth_date": date.fromisoformat("1994-04-03"),
                    "author_id": 34,
                }
            ]
        ),
        mock_glue_context,
    )


@patch("books_data_product.load_books.load_books")
@patch("books_data_product.load_books.load_authors")
@patch("books_data_product.load_books.save_books")
def test_main_stores_a_row_with_nulls_if_author_not_found(
    mock_save_books: Mock,
    mock_load_authors: Mock,
    mock_load_books: Mock,
    mock_glue_context: GlueContext,
):
    spark = mock_glue_context.spark_session
    mock_load_books.return_value = spark.createDataFrame(
        [{"title": "t", "publish_date": "2022-02-04", "author": "a"}]
    )
    mock_load_authors.return_value = spark.createDataFrame(
        [{"name": "not_a", "id": 34, "birth_date": "1994-04-03"}]
    )

    main(mock_glue_context)

    mock_save_books.assert_called_with(
        DataFrameMatcher(
            [
                {
                    "title": "t",
                    "publish_date": date.fromisoformat("2022-02-04"),
                    "author_name": "a",
                    "author_birth_date": None,
                    "author_id": None,
                }
            ]
        ),
        mock_glue_context,
    )


def test_save_books(mock_glue_context: GlueContext):
    sample_rows = [
        {
            "title": "t",
            "publish_date": date.fromisoformat("2022-02-04"),
            "author_name": "a",
            "author_birth_date": date.fromisoformat("1994-04-03"),
            "author_id": 34,
        }
    ]
    book_df = mock_glue_context.spark_session.createDataFrame(sample_rows)

    save_books(book_df, mock_glue_context)

    mock_glue_context.purge_table.assert_called_with(
        "glue_reference", "books", options={"retentionPeriod": 0}
    )
    mock_glue_context.write_dynamic_frame_from_catalog.assert_called_with(
        DynamicFrameMatcher(sample_rows),
        "glue_reference",
        "books",
    )
    purge_table_index = mock_glue_context.mock_calls.index(
        call.purge_table(ANY, ANY, options=ANY)
    )
    write_index = mock_glue_context.mock_calls.index(
        call.write_dynamic_frame_from_catalog(ANY, ANY, ANY)
    )
    assert purge_table_index < write_index
