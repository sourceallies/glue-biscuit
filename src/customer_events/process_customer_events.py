from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, when, isnull, lit
from datetime import datetime
from framework import merge_and_retain_last


def load_events(glue_context: GlueContext) -> DataFrame:
    pass


def load_customers(glue_context: GlueContext) -> DataFrame:
    pass


def save_customers(customers: DataFrame, glue_context: GlueContext):
    pass


def main(glue_context: GlueContext):
    existing = load_customers(glue_context)
    new_events = load_events(glue_context)
    new_events.show()
    new_records = new_events.select(
        coalesce(new_events.after.id, new_events.before.id).alias("id"),
        coalesce(new_events.after.first_name, new_events.before.first_name).alias("first_name"),
        coalesce(new_events.after.last_name, new_events.before.last_name).alias("last_name"),
        coalesce(new_events.after.email, new_events.before.email).alias("email"),
        (new_events.ts_ms / 1000).cast("timestamp").alias("_as_of"),
        when(isnull(new_events.after), True).otherwise(False).alias("_deleted"),
    )
    merged = merge_and_retain_last(new_records, existing,  
        key_fields=["id"],
        sort_field="_as_of",
        deleted_field="_deleted").drop("_deleted")
    merged.show()
    save_customers(merged, glue_context)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
