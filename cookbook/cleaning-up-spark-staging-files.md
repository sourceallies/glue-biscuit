
# Cleaning up Spark staging files

During execution of a Glue job, Spark will write temporary "staging" files to the target S3 bucket. Spark deletes these files at the end of the job, but if your target S3 bucket is versioned, these deleted files will remain in your bucket behind delete markers. Note that these files will be written at the root of the S3 target, regardless of the partitioning. These unnecessary files cost money to store and are inconvenient to delete if they are left over a long period of time given that they will accumulate in large numbers. A simple S3 lifecycle policy will avoid this cost and hassle.

An example lifecycle policy to permanently delete Spark staging files:
```
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
