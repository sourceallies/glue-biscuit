from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from framework.get_job_arguments import get_job_arguments
from pyspark.sql.functions import to_date, col


#  TODO: how do we ensure this returns the expected strucure
#  TODO: can we create a @cached decorator that will store the result if it is called multiple times
def load_books(glue_context: GlueContext) -> DataFrame:
    bucket_name = get_job_arguments("source_bucket")
    return glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [f"s3://{bucket_name}/sample_data/json/books"]},
        format="json",
    ).toDF()


# TODO: how do we ensure this saves the expected strucure
def save_books(books: DataFrame, glue_context: GlueContext):
    # Is there a way to remove the need to do this?
    df = DynamicFrame.fromDF(books, glue_context, "books")
    glue_context.write_dynamic_frame_from_catalog(df, "glue_reference", "raw_books")
    pass


def main(glue_context: GlueContext):
    books = load_books(glue_context)
    converted = books.select(
        "title",
        # G/P talked about ways to ensure structure of output. Protect agienst misnaming columns
        to_date(col("publish_date"), "yyyy-MM-dd").alias("publish_date"),
        col("author").alias("author_name"),
    )
    save_books(converted, glue_context)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
