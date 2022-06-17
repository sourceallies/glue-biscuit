from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame


def load_books(glue_context: GlueContext) -> DataFrame:
    return glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": ["s3://glue-reference-implementation-databucket-fed75mq4rmq0/sample_data/json/books"]},
        format="json"
    ).toDF()


def save_books(books: DataFrame, glue_context: GlueContext):
    df = DynamicFrame.fromDF(books, glue_context, 'books')
    glue_context.write_dynamic_frame_from_catalog(df, 'glue_reference', 'raw_books')
    pass


def main(glue_context: GlueContext):
    books = load_books(glue_context)
    books.show()


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
