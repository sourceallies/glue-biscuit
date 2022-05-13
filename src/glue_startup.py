from awsglue import DynamicFrame
from awsglue.context import GlueContext

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# glue
gc = GlueContext(sc)


def glue_dynamic_frame_crossjoin_demo(
    dyf1: DynamicFrame, dyf2: DynamicFrame
) -> DynamicFrame:
    return dyf1.join(["a", "b"], ["c", "d"], dyf2)


def glue_data_frame_demo(dyf1: DynamicFrame, dyf2: DynamicFrame) -> DynamicFrame:
    df1 = dyf1.toDF()
    df2 = dyf2.toDF()
    dfj = df1.join(df2, [col("a") == col("c"), col("b") == col("d")])
    dyf = DynamicFrame.fromDF(dfj, gc, "glue_df1")
    return dyf
