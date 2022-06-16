from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame


def load_books(gc: GlueContext) -> DataFrame:
    return gc.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://glue-reference-implementation-databucket-fed75mq4rmq0/sample_data/json/books"]},
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
