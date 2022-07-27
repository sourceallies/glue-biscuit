
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
