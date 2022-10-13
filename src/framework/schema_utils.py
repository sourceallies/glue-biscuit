from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from functools import wraps
from typing import Callable


def coerce_to_schema(df: DataFrame, schema: StructType):
    # removes extraneous fields
    result = df.select(schema.fieldNames())

    for field in schema.fields:
        result = result.withColumn(field.name, result[field.name].cast(field.dataType))

    return result


def __coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg


def __get_schema_args(*args):
    first_non_none = __coalesce(*args)
    if callable(first_non_none):
        return first_non_none()
    return first_non_none


def __force_frame_to_fit(schema, *args, **kwargs):
    new_args = [*args]
    for i in range(len(args)):
        if isinstance(new_args[i], DataFrame):
            new_args[i] = coerce_to_schema(new_args[i], schema)

    new_kwargs = {**kwargs}
    for k, _ in new_kwargs.items():
        if isinstance(new_kwargs[k], DataFrame):
            new_kwargs[k] = coerce_to_schema(new_kwargs[k], schema)
    return new_args, new_kwargs


def schema(*, schema_obj=None, schema_func=None):
    """
    Takes a pyspark schema or a function resulting in a pyspark schema and coerces the input frame to match this schema.
    """

    def annotation_func(func):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            final_schema = __get_schema_args(schema_obj, schema_func)
            new_args, new_kwargs = __force_frame_to_fit(final_schema, *args, **kwargs)
            return func(*new_args, **new_kwargs)

        return wrapper_func

    return annotation_func


def source(database: str, table: str, schema_obj=None, schema_func=None):
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)

    def annotation_func(func: Callable):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            final_schema = __get_schema_args(schema_obj, schema_func)
            dyn_frame = glue_context.create_dynamic_frame_from_catalog(
                database=database,
                table_name=table,
                transformation_ctx=f"source-{database}-{table}",
            )
            df = dyn_frame.toDF()
            fitted_frame = coerce_to_schema(df, final_schema)
            # Do we want to assume this is right here,
            # or do we want to do crazy reflection magic
            # to figure out a keyword arg? Both?
            return func(fitted_frame, *kwargs, **kwargs)

        return wrapper_func

    return annotation_func
