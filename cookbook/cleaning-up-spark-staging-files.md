
# Cleaning up Spark staging files

During execution of a Glue job, Spark will write temporary "staging" files to the target S3 bucket. Spark deletes these files at the end of the job, but if your target S3 bucket is versioned, these deleted files will remain in your bucket behind delete markers. These unnecessary files cost money to store and are inconvenient to delete if they are left over a long period of time given that they will accumulate in large numbers. A simple S3 lifecycle policy will avoid this cost and hassle.

An example lifecycle policy to permanently delete Spark staging files (using CDK in Python):
```
bucket.add_lifecycle_rule(
    id="spark-staging-files-expiration",
    expiration=cdk.Duration.days(1),
    noncurrent_version_expiration=cdk.Duration.days(1),
    prefix="s3_target_prefix/.spark-staging",
)
```
