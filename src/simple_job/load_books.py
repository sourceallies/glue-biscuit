from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame

def load_books(gc: GlueContext) -> DataFrame:
    pass


def save_books(books: DataFrame, gc: GlueContext):
    pass 


def main(gc: GlueContext):
    pass


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    gc = GlueContext(spark.sparkContext)
    main(gc)