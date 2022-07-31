from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def coerce_to_schema(df: DataFrame, schema: StructType):
    # removes extraneous fields
    result = df.select(schema.fieldNames())

    for field in schema.fields:
        result = result.withColumn(field.name, result[field.name].cast(field.dataType))

    return result
