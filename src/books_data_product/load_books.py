from ast import alias
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from framework import get_job_arguments
from pyspark.sql.functions import to_date, col


def load_books(glue_context: GlueContext) -> DataFrame:
    (bucket_name,) = get_job_arguments("source_bucket")
    source_path = f"s3://{bucket_name}/sample_data/json/books"
    print("Loading books from path: ", source_path)
    return glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="json",
    ).toDF()


def load_authors(glue_context: GlueContext) -> DataFrame:
    (bucket_name,) = get_job_arguments("source_bucket")
    source_path = f"s3://{bucket_name}/sample_data/json/authors"
    print("Loading books from path: ", source_path)
    return glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="json",
    ).toDF()


def save_books(books: DataFrame, glue_context: GlueContext):
    glue_context.purge_table("glue_reference", "books", options={"retentionPeriod": 0})
    df = DynamicFrame.fromDF(books, glue_context, "books")
    glue_context.write_dynamic_frame_from_catalog(df, "glue_reference", "books")


def main(glue_context: GlueContext):
    books = load_books(glue_context)
    books.printSchema()

    authors = load_authors(glue_context)
    authors.printSchema()
    joined = books \
        .join(authors.alias('a'), books.author == authors.name, "left_outer") \
        .select(
            'title',
            to_date(col("publish_date"), "yyyy-MM-dd").alias("publish_date"),
            col('author').alias('author_name'),
            to_date(col("a.birth_date"), "yyyy-MM-dd").alias("author_birth_date"),
            col('a.id').alias('author_id')
        )

    joined.printSchema()
    joined.show()
    save_books(joined, glue_context)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
