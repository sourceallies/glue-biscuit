from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# glue
gc = GlueContext(sc)


def get_data():
    df = gc.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={
            "paths": [
                "S3://oedi-data-lake/pv-rooftop/buildings/city_year=minneapolis_mn_07/part-00023-647f1a88-3404-4ce7-9c7f-f9e18fcfb3be.c000.snappy.parquet"
            ]
        },
        format="parquet",
    )
    return df
