from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

def source(glueContext: GlueContext) -> DataFrame:
    pass

def sink(glueContext: GlueContext, data: DataFrame):
    pass

def main(glueContext):
    pass

def get_desmoines_data(glueContext: GlueContext) -> DataFrame:
    return glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        # https://oedi-data-lake.s3.amazonaws.com/pv-rooftop/buildings/city_year=desmoines_ia_10/part-00076-647f1a88-3404-4ce7-9c7f-f9e18fcfb3be.c000.snappy.parquet
        connection_options={"paths": ["s3://oedi-data-lake/pv-rooftop/buildings/city_year=desmoines_ia_11/part-00076-647f1a88-3404-4ce7-9c7f-f9e18fcfb3be.c000.snappy.parquet"]},
        # connection_options={"paths": ["s3://awsglue-datasets/examples/us-legislators/persons_json"]},
        format="parquet"
    ).toDF()


def get_topeka_data(glueContext: GlueContext) -> DataFrame:
    return glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        # https://oedi-data-lake.s3.amazonaws.com/pv-rooftop/buildings/city_year=topeka_ks_08/part-00091-647f1a88-3404-4ce7-9c7f-f9e18fcfb3be.c000.snappy.parquet
        connection_options={"paths": ["s3://oedi-data-lake/pv-rooftop/buildings/city_year=topeka_ks_08/part-00091-647f1a88-3404-4ce7-9c7f-f9e18fcfb3be.c000.snappy.parquet"]},        
        format="parquet"
    ).toDF()


def save_output(glueContext: GlueContext, data: DataFrame):
    data.write.json('/tmp/output')


def main(glueContext: GlueContext):
    df = get_desmoines_data(glueContext)
    print(f'data1 count: {df.count()}')
    df2 = get_topeka_data(glueContext)
    print(f'data2 count: {df2.count()}')
    dfj = df.join(df2, 'gid')
    print(f'join count: {dfj.count()}')

    selected = dfj.select(df.bldg_fid, df.city, df.state, df.year)
    save_output(glueContext, selected)


# |-- gid: long
# |-- bldg_fid: long
# |-- the_geom_96703: string
# |-- the_geom_4326: string
# |-- city: string
# |-- state: string
# |-- year: long

#PRO-TIP: only run if this file is the entrypoint
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    gc = GlueContext(spark.sparkContext)
    main(gc)
