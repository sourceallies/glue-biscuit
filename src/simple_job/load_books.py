from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame


def load_books(glue_context: GlueContext) -> DataFrame:
    # TODO: make this an arg
    bucket_name = 'glue-reference-implementation-databucket-fed75mq4rmq0'
    return glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"s3://{bucket_name}/sample_data/json/books"]
        },
        format="json"
    ).toDF()


def save_books(books: DataFrame, gc: GlueContext):
    pass


def main(glue_context: GlueContext):
    books = load_books(glue_context)
    books.show()


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    gc = GlueContext(spark.sparkContext)
    main(gc)