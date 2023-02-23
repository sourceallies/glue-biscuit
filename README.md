# Glue Biscuit

[![Source Allies Logo](./doc_support/source-allies-logo-final.png)](https://sourceallies.com)

## Table of Contents

1. [Introduction](#introduction)
1. [Glue Job Structure](#glue-job-structure)
1. [Testing](#testing)
    1. [Unit Testing](#unit-testing)
1. [Cookbook](#cookbook)
    1. [Reprocessing data](#reprocessing-data)
    1. [Consuming event/change records into current state](#consuming-eventchange-records-into-current-state)
    1. [Support "point in time" queries via Partitioning](#support-point-in-time-queries-via-partitioning)
    1. [Data modeling as a denormilized table of many columns](#data-modeling-as-a-denormilized-table-of-many-columns)
    1. [Cleaning up Spark staging files](#cleaning-up-spark-staging-files)
    1. [Only processing un-processed data](#only-processing-un-processed-data)

## Introduction

AWS Glue provides a serverless runtime for Apache Spark. 
It extends Spark with additional capabilities that are specific to the AWS ecosystem.

The use cases for this service can be broken down into two broad catagories:

The first is ad-hoc and manual use of Glue for analysis or to do wrangling of data before loading into a system.
These uses are successful because there is a human that is able to look at the output and use their judgement to modify the code on the fly to produce the desired results.

The other involves using Glue as part of a production data pipeline.
In this use case Glue Jobs are expected to run un-attended, reliably, and accurately.
Error in the output of these jobs can have subtle but catastrophic effects on downstream systems.
Depending on the results, these errors may not be noticed for several days or weeks.
These jobs may also be very complicated and need to handle many different data scenarios.

In this second class of use cases, Glue jobs are no longer one-off scripts, but rather software applications.
Therefore, we need to apply a software engineering process.
This document is our attempt to explain the best practices for how to create maintainable, testable, and scalable Glue jobs for a professional development organization. 
It includes a companion library to help software teams achieve these goals.

This guide assumes the reader has:
- A general understanding of Python
- Familiarity with AWS and the common services (IAM, S3, CloudFormation)
- Created a basic Glue job, possibly as part of a tutorial
- An understanding of the concept of a Continuous Deployment pipeline and why they are used within software development
- Familiarity with automated unit testing

## Glue Job Structure

In Glue, a job is a Python file and associated configuration that can be executed within AWS.
If a job is created via a template in the Glue Console or a tutorial then the resulting Python file is usually a simple script that follows a top-down progression of execution in this order:
1. Import statements
2. Create a `SparkSession`, `GlueContext`, and possibly `Job` objects
3. Call one of the `GlueContext.create_dynamic_frame_*` methods to read data
4. Call various `DynamicFrame` or `DataFrame` methods to transform the data
5. Write out the results using one of the `GlueContext.write_dynamic_frame*` methods
6. Optionally, commit the `Job` if using bookmarks

This structure creates some friction as the job evolves.
In order to ensure that this job does not produce invalid results, we need a way of testing it.
If a unit test is written and attempts to import this file, the job will immediately begin executing.
This does not allow an opportunity for data to be mocked or for the job to be run multiple times in various scenarios.
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

Realistically, Glue jobs grow more complex and will potentially need to read from multiple sources, do various operations and save data multiple destinations.
There are also opportunities to extract out boilerplate, cross-cutting functionality that is not specific to a job.
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
    The sign of a well-designed job is when the Main method reads like a high level flow chart of how the job executes.

By breaking the job down, we can now independently test that each Data Source function is loading data from the correct place with the correct parameters.
We can then test that the Data Sinks will call the appropriate Glue APIs correctly.
Given that the Sources and Sinks are tested as correct, they can be mocked out when testing the main function.
This mocking allows the test cases to provide different inputs to the job and execute all the code paths and testing scenarios of the main function.

## Testing

### Unit Testing

In order to unit test Glue jobs, we recommend trying to execute as much of the Glue and Spark code as possible. 
This is because most real world jobs are highly dependent on using various Spark methods in order to perform their task.
If we were to mock these methods and only test the code that is physically in the job file then we would be relying on many assumptions about the behavior of Spark. 
In addition, our tests would be very tightly coupled to the implementation of the job, making the tests brittle as the job evolves over time.

We test each source, sink, and the main method individually.


In order to test the source methods, we need to provide it an implementation of `GlueContext` that does not attempt to call out to the network.
The `framework.test` module provides a `mock_glue_context` fixture that can be imported and referenced.

Most source methods also use job arguments for various runtime parameters. 
The Glue provided `getResolvedOptions` can be mocked, however this method is tricky to mock and inconvenient to use. 
Instead, we recommend using the framework-provided `get_job_argument` or `get_job_arguments` functions. 
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

The sink tests needed to assert the correct DynamicFrame was passed to Glue.
The main function tests will need to assert that the correct DataFrame was passed to the sinks.
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
Fortunately there is a Docker image provided by AWS that contains a full Glue runtime.
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

## Cookbook

A series of designs on how to approach common problems.

### Reprocessing data

Reprocessing data within a data lake presents certain challenges. If you think of your traditional bronze/silver/gold data lake architecture, this will only apply to the silver and gold layers. The bronze layer should never be modified to allow re-creation of the more curated levels.

#### Pattern

The general pattern for reprocessing data is as follows:

1. Identify the impacted partitions
1. If reprocessing raw events into the silver layer, set job bookmarks as narrowly as possible to reduce the incoming data-set.
1. Take note of the job start time.
1. Re-build the entire target partition
1. Write the data-set to the same location as the target data-set
1. Delete all files in the partition location that existed before the job began

#### Pseudo-Code

```Python
job_start = datetime.utcnow()
df = read_input_data(*partition_values)
final_df = build_partition(df)
write_output_data(final_df)
delete_files_before(job_start)
```

#### Partitioning

Care should be taken when designing the data lake to design partitions that limit the blast radius of the reprocessing of any partitions. Analyze access patterns and potential damage a malformed event could cause when processed by the data lake. A good rule of thumb is that a bad event should only cause the reprocessing of a single partition in the silver layer, and only one partition in each curated data product within the gold layer. This will reduce cost and time when recovering from a bad event.

#### Idempotency

Idempotency, or the ability to apply a process multiple times without changing the end state, is the end goal of our strategy. This concept is particularly important when creating a data lake. Having all operations be idempotent allows us to re-build the lake from the raw events by merely re-running our jobs. 

With relation to partitions, this is important because it allows us to re-build a partition by just re-running a single job, instead of performing data surgery for hours beforehand.

#### Bronze/Silver/Gold Architecture

The Bronze/Silver/Gold architecture is a common pattern for data lakes. In it, we model our data in a series of layers, starting with the low quality data at one end (Bronze), and use ETL jobs to progressively enhance the data as it moves between layers (Silver) until we have a quality data product that is ready for end user interaction (Gold).

For example, we may have a raw series of events landed in an S3 bucket. This would be our bronze layer. We might use a series of glue jobs to remove nulls and separate the raw events into conceptual tables in the silver layer. Finally another series of jobs would read data from the silver layer and create a data product that is useful for our ultimate consumers in the gold layer. This final layer would be the part that our users would interact with.

Despite the name, this architecture should not limit you to three layers. There can be as many intermediate layers as your data processing needs require.

### Consuming event/change records into current state

One common use case for a Glue job is to consume a data source that consists of records that represent events or changes to some entity in a source system. 
These events are often generated out of some sort of [change data capture](https://en.wikipedia.org/wiki/Change_data_capture) framework like [Debezium](https://debezium.io) or [Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/Welcome.html).
The goal is to process new events into a dataset where each row represents the state of an entity at a point in time. Usually, this is the current time.

There are several corner cases that have to be handled properly in order for this job to produce correct results:
- One run may pick up multiple events for the same entity
- Some entities that were previously loaded may not have any events
- An event could indicate the deletion of an entity
- Events could be picked up in multiple jobs and actually represent an older state of the target entity.

In order to support this pattern, both input and target records must satisfy a couple criteria:
- Each record must have a set of columns that uniquely identify the entity that is being manipulated. This is typically the columns that are the primary key in the source.
- Each record must have a timestamp that orders the mutations.
- Optionally, to support deletes, each record needs a way to identify that it is a delete.

This is the logical algorithm to process this data:
1. New records are loaded from the source (usually via a Job Bookmark).
1. Source events are converted into the target structure. As part of this conversion, populate an `effective_date` column with the event time and a `_deleted` flag with `true` if the event represents a deletion and `false` otherwise.
1. Load the current target dataset.
1. Add a `_deleted` column to the target with a value of `false`.
1. Union the source and target.
1. Execute a group-by on the primary key columns.
1. For each group, reduce the group by selecting the record with the largest timestamp (most current record). 
1. This resulting dataset now has exactly one record per entity.
1. Filter out any records with a `_deleted` flag that is `true`.
1. Drop the `_deleted` column.
1. Write out the output to the target.
1. Purge data from the target that existed prior to the job run.

In order to help with the execution of the above algorithm, we provide a [merge_and_retain_last](src/framework/merge_utils.py) function to help with the middle steps between adding a `_deleted` column and dropping it. 

### Support "point in time" queries via Partitioning

Many times, users have the need to query data as it existed historically.
In traditional data warehouses, this was often implemented as "effective" and "term" dates. 
These dates were added to `where` clauses in order to restrict the rows returned to only those effective at the desired time.
When loading data, existing records are "termed" and records with the current state are inserted with new effective dates.

Within a data lake that is backed by files in S3, this architecture creates two problems:
During a load process, we cannot simply update individual records in S3, instead, entire files must be re-written.
Since AWS Glue and other technologies that read from the data lake do not support indexes in the traditional sense, they cannot effectively filter out records that are not current without scanning all the data in the lake. 

Rather than using effective and term dates, we can instead decide on a "granularity" and then create a partition for each value of that granularity.
For example, if we need to query data by month, we would create a partition for each month and load an entire copy of the data into this partition. 
This creates a copy of the entire dataset every month, but since Parquet files are highly space-efficient and S3 storage is very cheap this is not a concern.
When users want to run a point in time query, they simply specify the partition for the month of concern.

The general strategy for loading data is as follows:
1. Union the incoming data, the data for the previous partition, and the current partition.
1. For each unique key, retain the most recent record
1. Drop any records that have delete markers
1. Write back out the current partition
1. Purge any old files from the current partition (prior to the job start).

If the definition of "current" is not based on ingestion time, but rather some sort of business date (ex. order date, activation date, etc) then the above process is more complicated.
1. As incoming data is processed, the earliest affected partition needs to be identified. 
1. The incoming data that is effective during that window can be merged into that partition and the partition is rebuilt.
1. The data then needs to be "rolled forward" into the next partition.
1. This process is repeated for each partition until the current partition is reached.

### Data modeling as a denormalized table of many columns


A common use for AWS Glue is to process data within a Data Lake environment.
In these architectures data is typically stored in S3 as Parquet files.
For teams that are used to working with the Dimensional or Snowflakes of a data warehouse, they may be tempted to create a similar data structure in the Data Lake. 
This would take the form of a table within the Glue catalog for each entity and relationship.
We recommend against this.

The constraints within a S3, Parquet-based lake are not the same as those on a relational database.
- Joins are expensive
- Individual rows cannot be updated. Instead, entire partitions are rewritten or appended to.
- A large amount of data can be scanned in one pass.
- Parquet files automatically de-duplicate and compress data.

Start the data modeling with a "Data Product".
Decide on a single table that would provide value to end users.
Like a dimensional model, decide on the ["Grain"](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/) of this table. 
Once this is decided, an iterative process of development can start with the primary key of this table and a few attributes of that entity.
As the team iterates, they can expand the product by adding additional columns as needed.
The only constraint is that they cannot change the grain.
For example, if the grain of a row is "one payment on a policy" then there should never be more than one row that represents the same payment and there should never be a payment that does not have a row.

At a certain point, there will become a time where related data becomes useful. 
Continuing our example, if a `Payments` table has one row for each payment on a policy, then data about the policy might be useful, or data about the policy holder. 
If this was a data warehouse, additional tables (i.e. dimensions) would be created and referenced in order to hold this data.
The argument is that this data is "duplicated" across all the policies and needs to be held in a table of its own. 
In this model, we would instead continue to add columns to the `Payments` table. 
The data would be duplicated through each payment. 
So if a policy holder made 100 payments, then yes, the name, address, phone number, etc would be duplicated 100 times.

There may also be source data where there are multiple values per row.
If this is the case (like a `label` field that could be 0 - many strings) then the team should consider the various data structure types supported by Parquet as well as Glue and Athena. 
Unlike a relational database, there is no indexing advantage to putting a list of strings in a separate table, instead, if it is stored as an `array<string>` column then it can be queried just like any other column and the user is not forced to join it to a parent table to produce useful data.

The knee-jerk reaction to this is that it is a "bad" design because of all this duplication and that it would cause a lot of wasted space, performance problems, data correctness issues, etc.
In fact, this design makes the Data Lake both simpler to build and faster to use. 
- Parquet will automatically deduplicate column values within itself. 
    This means that it doesn't matter if we store the same name 100s of times, the actual space will be far less. 
- This "wide table" strategy also simplifies the process of loading data: 
    Rather than having a bunch of separate jobs for each of several tables, One job can be created that joins the data together and loads the single table.
- Data is easier to reason about:
    Users don't have to know what columns to join on and make sure join criteria are correct to prevent issues.
    They can simply query the table and group or filter by any available columns.
- With multiple tables, some data could be loaded into one table while other tables have not yet been loaded or their loads failed.

There is a limit to this setup. 
The table can have hundreds, but probably not thousands of columns. 
There should be some consideration for the users and use cases for the data. 
Where a traditional Data Warehouse has "Data Marts" that are copies of data for different audiences, a Data Lake can also have different tables that are focused on different use cases where needed. 
This does not remove the need to sometimes have other tables at different grains.
In our example there is still probably going to be a `PolicyHolders` table that has one row per customer.
An important distinction is that these two tables are fully independent and not tied together like in a Dimensional or Snowflake design.

### Removing sensitive values from datasets (PII)

There are a variety of options for dealing with PII when building data pipelines depending on the use case.

#### Methods

##### Removal

The first and simplest solution is to remove the fields containing PII altogether. Make sure your data product needs to include the sensitive data before going through the effort of implementing one of the solutions below.

##### Encryption

The second option is to encrypt the sensitive data and store the encoded ciphertext in your dataset. This solution is suitable when the sensitive values are larger, have few repeated values, and do not need to be joined to other tables or datasets, i.e. an email body. The [AWS Encryption SDK](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/introduction.html) can be used with a KMS key to encrypt the data using an envelope encryption method. Note that values encrypted with the AWS Encryption SDK are most easily decrypted using the SDK due to this proprietary method. By default, the SDK generates a new data key for each value encrypted via a network call. When encrypting large datasets it is important to configure a [cache](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/data-key-caching.html) to reuse data keys and reduce network calls.

There are several ways to integrate this encryption process into your ETL. If the data is processed in small enough batches, the `collect` function can be used on your dataframe to bring the necessary data into the memory of the driver node to encrypt the data using the SDK. This method is very simple and suitable when the data is processed in batches of less than 1 GB. If the data is too large to process on the driver node, a Step Function can be used to parallelize the decryption work using Lambdas.

##### Tokenization

When the sensitive data is small, will be encrypted many times as new data comes in, and can be used to join datasets, then tokenization is a good option. Tokenization involves encrypting the data and assigning it a unique identifier that will act as the token before storing it in a database such as DynamoDB. Subsequent runs should check to see if the value has been tokenized before creating a new token. This can be implemented as a separate service and shared among teams.

##### Redaction

Redaction is used when the majority of the value is desired, but the value can contain sensitive data. [AWS Comprehend](https://aws.amazon.com/comprehend/) has features to do PII detection and removal. Type of PII can be specified, for example, SSN, Name, or Credit Card Numbers. The redacted text can be replaced with the type of PII that was removed. This can be integrated with Athena for ease of use ([tutorial](https://aws.amazon.com/blogs/machine-learning/translate-and-analyze-text-using-sql-functions-with-amazon-athena-amazon-translate-and-amazon-comprehend/)).


When using any of these methods, think carefully about how sensitive data is stored before it is encrypted. Avoid writing the unencrypted data to disk altogether by using a Lambda triggered by a Kinesis stream. If you must stage the data before encrypting it, make sure to clean it up afterwards either using API calls or via an S3 lifecycle policy.

### Cleaning up Spark staging files


During execution of a Glue job, Spark will write temporary "staging" files to the target S3 bucket. Spark deletes these files at the end of the job, but if your target S3 bucket is versioned, these deleted files will remain in your bucket behind delete markers. Note that these files will be written at the root of the S3 target, regardless of the partitioning. These unnecessary files cost money to store and are inconvenient to delete if they are left over a long period of time given that they will accumulate in large numbers. A simple S3 lifecycle policy will avoid this cost and hassle.

An example lifecycle policy to permanently delete Spark staging files:
```json
"LifecycleConfiguration": {
  "Rules": [
    {
      "ExpirationInDays": 1,
      "Id": "spark-staging-files-expiration",
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 1
      },
      "Prefix": ".spark-staging",
      "Status": "Enabled"
    }
  ]
}
```

### Only processing un-processed data

During the creation of a data lake, you *will* make mistakes. At some point you're going to have to re-process some amount of data. Doing this without re-processing the entire dataset is an important feature of building a well performing data lake.

The important feature for this in Glue is "Bookmarks". Bookmarks allow you to keep track of what data has already been processed, and to re-run jobs with a subset of the complete dataset.

#### Job Creation

When creating the job, you must pass the following as a parameter to your python script:

`--job-bookmark-option job-bookmark-enable`

This will cause Glue to keep track of what data has been processed, and only read in data that has not yet been fed through your job.

#### Python Code

To register what data has been already processed, you need to add the following code to your job:

```python
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#####################
# ETL logic goes here
#####################

job.commit()
```

To use bookmarks, a job **must** have a name.

Once you call commit, Glue remembers that your application has processed the data, and will not deliver it to you again. Do NOT call commit in the failure case for your job.

#### Running A Glue Job For A Subset Of Data

Bookmarks also give you the ability to re-run a subset of your total dataset. This is based on the time in which previous jobs have run, and requires pausing the bookmark bookkeeping that Glue does.

To re-process all of the data from when job run A ran (inclusive) up until job run B happened (exclusive), you need to pass the following arguments to your job:

- `--job-bookmark-option job-bookmark-pause`
- `--job-bookmark-from {job_run_id_A}`
- `--job-bookmark-to {job_run_id_B}`

#### Pitfalls

Bookmarks do not make your job atomic. If you write data and your job fails before calling `job.commit()`, the data is still written. This is not a replacement for making your job idempotent.
