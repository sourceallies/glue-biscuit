from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import sys


def load_books(glue_context: GlueContext) -> DataFrame:
    # TODO: This is nasty
    print('using argv', sys.argv)
    bucket_name = getResolvedOptions(sys.argv, ['source_bucket'])['source_bucket']
    return glue_context.create_dynamic_frame_from_options(
        connection_type='s3',
        connection_options={'paths': [f's3://{bucket_name}/sample_data/json/books']},
        format='json'
    ).toDF()


def save_books(books: DataFrame, glue_context: GlueContext):
    df = DynamicFrame.fromDF(books, glue_context, 'books')
    glue_context.write_dynamic_frame_from_catalog(df, 'glue_reference', 'raw_books')
    pass


def main(glue_context: GlueContext):
    books = load_books(glue_context)
    converted = books.select(
        'title',
        to_date(col('publish_date'), 'yyyy-MM-dd').alias('publish_date'),
        col('author').alias('author_name')
    )
    save_books(converted, glue_context)


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
