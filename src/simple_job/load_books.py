from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from glue_biscuit import get_job_arguments
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, StringType
from glue_biscuit.schema_utils import sink, schema_from_glue, schema


#  TODO: can we create a @cached decorator that will store the result if it is called multiple times
def load_books(glue_context: GlueContext) -> DataFrame:
    (bucket_name,) = get_job_arguments("source_bucket")
    source_path = f"s3://{bucket_name}/sample_data/json/books"
    print("Loading books from path: ", source_path)
    return glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="json",
    ).toDF()


@sink(
    "glue_reference",
    "raw_books",
    lambda: schema_from_glue("glue_reference", "raw_books"),
)
def save_books(books: DataFrame, glue_context: GlueContext):
    glue_context.purge_table(
        "glue_reference", "raw_books", options={"retentionPeriod": 0}
    )
    return books


@schema(
    schema_obj=StructType(
        [
            StructField("title", StringType()),
            StructField("publish_date", StringType()),
            StructField("author", StringType()),
        ]
    )
)
def transform_books(books: DataFrame) -> DataFrame:
    return books.select(
        "title",
        to_date(col("publish_date"), "yyyy-MM-dd").alias("publish_date"),
        col("author").alias("author_name"),
    )


def main(glue_context: GlueContext):
    books = load_books(glue_context)
    converted = transform_books(books)
    save_books(converted, glue_context)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
