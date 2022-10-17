from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from framework import get_job_arguments


def load_events(glue_context: GlueContext) -> DataFrame:
    pass


def load_customers(glue_context: GlueContext) -> DataFrame:
    pass


def save_customers(customers: DataFrame, glue_context: GlueContext):
    pass


def main(glue_context: GlueContext):
    pass


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
