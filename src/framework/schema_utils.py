import re
from typing import List
from cfn_tools import load_yaml
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
    IntegerType,
    LongType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType,
)
from pyspark.sql.types import StructType
from functools import wraps
from typing import Callable


def coerce_to_schema(df: DataFrame, schema: StructType):
    # removes extraneous fields
    result = df.select(schema.fieldNames())

    for field in schema.fields:
        result = result.withColumn(field.name, result[field.name].cast(field.dataType))

    return result


def schema_from_glue():
    pass


def __get_glue_table(resources: dict, table_name: str):
    for _logical_id, resource_definition in resources.items():
        try:
            name = resource_definition["Properties"]["TableInput"]["Name"]
            if resource_definition["Type"] == "AWS::Glue::Table" and name == table_name:
                return resource_definition
        except KeyError:
            pass


class BadTypeException(Exception):
    pass


def __handle_decimal(match: re.Match):
    (precision, scale) = match.groups()
    return DecimalType(int(precision), int(scale))


def __handle_hive_struct_field(struct_field: str):
    [name, field_type] = struct_field.split(":")
    return StructField(name, __convert_type(field_type))


def __handle_hive_struct(hive_struct_types: str):
    return [__handle_hive_struct_field(field) for field in hive_struct_types.split(",")]


def __handle_struct(match: re.Match):
    (struct_fields,) = match.groups()
    return StructType(__handle_hive_struct(struct_fields))


def __handle_array(match: re.Match):
    (array_type,) = match.groups()
    return ArrayType(__convert_type(array_type))


def __convert_type(column_type: str):
    options = [
        (lambda t: t == "string", lambda _match: StringType()),
        (lambda t: t == "tinyint", lambda _match: ByteType()),
        (lambda t: t == "smallint", lambda _match: ShortType()),
        (lambda t: t == "int", lambda _match: IntegerType()),
        (lambda t: t == "bigint", lambda _match: LongType()),
        (lambda t: t == "date", lambda _match: DateType()),
        (lambda t: t == "timestamp", lambda _match: TimestampType()),
        (lambda t: re.match(r"decimal\((\d+),(\d+)\)", t), __handle_decimal),
        (lambda t: re.match(r"^struct<(.*)>$", t), __handle_struct),
        (lambda t: re.match(r"^array<(.*)>$", t), __handle_array),
    ]
    for test_func, type_func in options:
        match = test_func(column_type.lower())
        if match:
            return type_func(match)
    raise BadTypeException(f"could not find type for {column_type}")


def __convert_column(column):
    return StructField(column["Name"], __convert_type(column["Type"]))


def __convert_columns(columns: List[dict]):
    return [__convert_column(column) for column in columns]


def schema_from_cloudformation(path_to_template: str, table_name: str) -> StructType:
    template_text = open(path_to_template, "r").read()
    parsed_template = load_yaml(template_text)
    resources = parsed_template["Resources"]
    table = __get_glue_table(resources, table_name)
    return StructType(
        __convert_columns(
            table["Properties"]["TableInput"]["StorageDescriptor"]["Columns"]
        )
    )


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
        if isinstance(new_args[i], DynamicFrame):
            new_args[i] = new_args[i].toDF()
        if isinstance(new_args[i], DataFrame):
            new_args[i] = coerce_to_schema(new_args[i], schema)

    new_kwargs = {**kwargs}
    for k, _ in new_kwargs.items():
        if isinstance(new_kwargs[k], DynamicFrame):
            new_kwargs[k] = new_kwargs[k].toDF()
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
