
## Introduction

AWS Glue provides a serverless runtime for Apache Spark. 
It extends Spark with additional capabilities that are specific to the AWS ecosystem.

The use cases for this service can be broken down into two broad catagories:

The first is someone wanting to do ad-hoc analysis.
Or, a team may need to do some manaual wrangling of data before loading it into a system.
These uses are successful because there is a human that is able to look at the output and use their judgement to modify the code on the fly to produce the desired results.

The other involves using Glue as part of a production data pipeline.
Glue Jobs are expected to run un-attended, reliably, and accuratly.
Error in the output of these jobs can have subtle but catistrophic effects on downstream systems.
Depending on the results, these errors may not be noticed for several days or weeks.
These jobs may also be very complicated and need to handle many different data scenerios.

In this second class of use cases, Glue jobs are no longer one-off scripts, but rather software applications.
Therefore, we need to apply a software engineering process.
This document is our attempt to explain the best practices for how to create maintainable, testable, and scalable Glue jobs for a professional development organization. 
It includes a companion library to help software teams achieve these goals.

This guide assumes the reader has:
- A general understanding of Python
- Familiarity with AWS and the common services (IAM, S3, Cloudformation)
- Created a basic Glue job. Possibly as part of a tutorial
- The concepts of a Continuious Deployment pipeline and why they are used within software development
- Familiarity with automated unit testing

## Glue Job Structure

In Glue, a job is a Python file and associated configuration that can be executed within AWS.
If a job is created via the Glue Console using a template, or a tutorial is followed then the resulting Python file is usually a simple script that follows a top down progression of execution in this order:
1. Import statements
2. Create a `SparkSession`, `GlueContext`, and possibly `Job` objects
3. Call one of the `GlueContext.create_dynamic_frame_*` methods
4. Call various `DyanmicFrame` or `DataFrame` methods to transform the data
5. Write out the results using one of the `GlueContext.write_dynamic_frame*` methods
6. Optionally, commit the `Job` if using bookmarks

This structure creates some friction as the job evolves.
In order to ensure that this job does not produce invalid results, we need a way of testing it. 
If a unit test is written and attempts to import this file, the job will immeditaly begin executing.
This does not allow an opportunity for data to be mocked or for the job to be run multiple times in various scenerios.
To address this limitation, we give up the testing of a small piece of the job.
Specifically, the creation of the `SparkSession` and `GlueContext`
We do this by wrapping the entire job into a function and calling it only when the script is the entrypoint of execution.
This provides a function that can be imported into a unit test and executed multiple times.

```Python
def main(glue_context: GlueContext):
    # Do all the data processing with the provided context


if __name__ == "__main__":
    # If this is the entrypoint, create a new SparkSession/GlueContext and then execute the main part of the job
    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    main(glue_context)
```

As the jobs grow into more realistic levels of complexity, they need to read from multiple sources, do various operations and save data to possibly multiple destinations.
There are also opportunities to extract out boilerplate, cross cutting, functionality that is not specific to a job.
In order to make the job more scalable, we further break down the `main` function into three categories of functions:
1. **Data Sources**: These take the form of `def load_something(glue_context: GlueContext) -> DataFrame:`. 
    These functions should just determine the parameters needed and load data into a Spark `DataFrame`, not do any processing. 
    We want to create a distinct function for each `DataFrame` we want to load.
2. **Data Sinks**: These take the form of `def save_something(some_data: DataFrame, glue_context: GlueContext):`. 
    These functions should simply store data using the various `GlueContext.write_dynamic_frame*` methods.
    There is usually only one per job, but could be more.
3. **Main**: This is the coordinator for the job.
    It should call the Data Sources to get data, process it and then call the Data Sinks as needed.
    Following the premises of clean code, private helper functions should be pulled out and added as needed in order to keep the job maintainable and understandable.
    The sign of a well designed job is when the Main method reads like a high level flow chart of how the job executes.

By breaking the job down, We can now independently test each Data Source function is loading data from the correct place, with the correct parameters.
We can then test that the Data Sinks will call the appropriate Glue APIs correctly.
Given that the Sources and Sinks are tested as correct, they can be mocked out when testing the main function.
This mocking allows the test cases to provide different inputs to the job and execute all the code paths and testing scenerios of the main function.

## Testing

### Unit Testing

In order to Unit Test Glue jobs, we recommend trying to execute as much of the Glue and Spark code as possible. 
This is because most real world jobs are highly dependent on using various Spark methods in order to perform their task.
If we were to mock these methods and only test the code that is physically in the job file then we would be relying on many assumtpions about the behavior of Spark. 
In addtion, our tests would be very tightly coupled to the implementation of the job.
Making the tests brittle as the job evolves over time.

We test each source, sink, and the main method individually.


In order to test the source methods, we need to provide it an implementation of `GlueContext` that does not attempt to call out to the network.
The `framework.test` module provides a `mock_glue_context` fixture that can be imported and referenced.

Most source methods also use job arguments for various runtime parameters. 
The Glue provided `getResolvedOptions` can be mocked, however this method is tricky to mock and actually pretty inconvient to use. 
Instead, we recommend using the framework provided `get_job_argument` or `get_job_arguments` functions. 
The `get_job_argument` function takes a string and returns the value of that argument. 
The plural form `get_job_arguments` takes any number of argument names as parameters and returns a tuple with the values in order.
We can then simply mock one of these methods and return the appropriate value.

Here is an example unit test for a source function:
```Python
from framework.test import mock_glue_context

@patch("simple_job.load_books.get_job_argument")
def test_load_books(mock_get_job_argument: Mock, mock_glue_context: GlueContext):
    mock_get_job_argument.return_value = "mock_bucket"
    mock_data = mock_glue_context.create_dynamic_frame_from_rdd(
        mock_glue_context.spark_session.sparkContext.parallelize([{"a": 1}]),
        "sample input",
    )
    mock_glue_context.create_dynamic_frame_from_options.return_value = mock_data

    actualDF = load_books(mock_glue_context)

    mock_get_job_arguments.assert_called_with("source_bucket")
    mock_glue_context.create_dynamic_frame_from_options.assert_called_with(
        connection_type="s3",
        connection_options={"paths": ["s3://mock_bucket/sample_data/json/books"]},
        format="json",
    )
    expectedData = [{"a": 1}]
    assert type(actualDF) is DataFrame
    assert [row.asDict() for row in actualDF.collect()] == expectedData
```

Data sink functions are tested in much the same way as source functions.
In order to test that the appropriate data was passed to one of the `GlueContext` write methods, the framework provides a `DynamicFrameMatcher` class.

Here is an example sink function test:
```Python
def test_save_books(mock_glue_context: GlueContext):
    book_df = mock_glue_context.spark_session.createDataFrame(
        [
            {
                "title": "t",
                "publish_date": date.fromisoformat("2022-02-04"),
                "author_name": "a",
            }
        ]
    )

    save_books(book_df, mock_glue_context)

    mock_glue_context.purge_table.assert_called_with(
        "glue_reference", "raw_books", options={"retentionPeriod": 0}
    )
    mock_glue_context.write_dynamic_frame_from_catalog.assert_called_with(
        DynamicFrameMatcher(
            [
                {
                    "title": "t",
                    "publish_date": date.fromisoformat("2022-02-04"),
                    "author_name": "a",
                }
            ]
        ),
        "glue_reference",
        "raw_books",
    )
    purge_table_index = mock_glue_context.mock_calls.index(
        call.purge_table(ANY, ANY, options=ANY)
    )
    write_index = mock_glue_context.mock_calls.index(
        call.write_dynamic_frame_from_catalog(ANY, ANY, ANY)
    )
    assert purge_table_index < write_index
```

Most source and sink functions will only have a couple of test cases.
They are, by design, very simple functions and mostly serve to hide the specifics of how data is loaded and stored.
They also allow us to individually mock them and return different values when testing the main method.
Without these functions we would have to mock `GlueContext.create_dynamic_frame_from_options` and dynamically return different test data for each invocation.

The sink tests needed to asser the correct DyanmicFrame was passed to Glue.
The main function tests will need to asser that the correct DataFrame was passed to the sinks.
For this purpose the framework provides the `DataFrameMatcher` class.

Here is a simple example of a test that mocks two sources and a sink and then tests the main method properly joins a single row.

```Python
@patch("books_data_product.load_books.load_books")
@patch("books_data_product.load_books.load_authors")
@patch("books_data_product.load_books.save_books")
def test_main_joins_and_writes_a_row(
    mock_save_books: Mock,
    mock_load_authors: Mock,
    mock_load_books: Mock,
    mock_glue_context: GlueContext,
):
    spark = mock_glue_context.spark_session
    mock_load_books.return_value = spark.createDataFrame(
        [{"title": "t", "publish_date": "2022-02-04", "author": "a"}]
    )
    mock_load_authors.return_value = spark.createDataFrame(
        [{"name": "a", "id": 34, "birth_date": "1994-04-03"}]
    )

    main(mock_glue_context)

    mock_load_books.assert_called_with(mock_glue_context)
    mock_load_authors.assert_called_with(mock_glue_context)
    mock_save_books.assert_called_with(
        DataFrameMatcher(
            [
                {
                    "title": "t",
                    "publish_date": date.fromisoformat("2022-02-04"),
                    "author_name": "a",
                    "author_birth_date": date.fromisoformat("1994-04-03"),
                    "author_id": 34,
                }
            ]
        ),
        mock_glue_context,
    )
```

With these basics in place, test files can iterate on different inputs to the main function (via mocks) and then assert the various sinks. 
Pytest fixtures can be extracted as needed to create test `DataFrame`s

In order to execute these tests, Glue, Pyspark, and Pytest all must be present. 
Fortunatly there is a Docker image provided by AWS that contains a full Glue runtime.
We can simply launch this container and use it to execute tests like so:

```bash
docker run \
    -v "$$(pwd):/work" \
    -w /work \
    -e DISABLE_SSL=true \
    -e PYTHONPATH='/home/glue_user/aws-glue-libs/PyGlue.zip:/home/glue_user/spark/python/lib/py4j-0.10.9-src.zip:/home/glue_user/spark/python/:/work/src' \
    --entrypoint=/home/glue_user/.local/bin/pytest \
    amazon/aws-glue-libs:glue_libs_3.0.0_image_01
```