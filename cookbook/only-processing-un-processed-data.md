# Only Processing Un-Processed Data

During the creation of a data lake, you *will* make mistakes. At some point you're going to have to re-process some amount of data. Doing this without re-processing the entire dataset is an important feature of building a well performing data lake.

The important feature for this in Glue is "bookmarks". Bookmarks allow you to keep track of what data has already been processed, and to re-run jobs with a subset of the complete dataset.

## Job Creation

When creating the job, you must pass the following as a parameter to your python script:

`--job-bookmark-option job-bookmark-enable`

This will cause Glue to keep track of what data has been processed, and only read in data that has not yet been fed through your job.

## Python Code

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

## Running A Glue Job For A Subset Of Data

Bookmarks also give you the ability to re-run a subset of your total dataset. This is based on the time in which previous jobs have run, and requires pausing the bookmark bookkeeping that Glue does.

To re-process all of the data from when job run A ran (inclusive) up until job run B happened (exclusive), you need to pass the following arguments to your job:

- `--job-bookmark-option job-bookmark-pause`
- `--job-bookmark-from {job_run_id_A}`
- `--job-bookmark-to {job_run_id_B}`
