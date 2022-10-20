# Reprocessing Data

Reprocessing data within a data lake presents certain challenges. If you think of your traditional bronze/silver/gold data lake architecture, this will only apply to the silver and gold layers. The bronze layer should never be modified to allow re-creation of the more curated levels.

## Partitioning

Care should be taken when designing the data lake to design partitions that limit the blast radius of the reprocessing of the reprocessing of any partitions. Analyze access patterns and potential damage a malformed event could cause when processed by the data lake. A good rule of thumb is that a bad event should only cause the reprocessing of a single partition in the silver layer, and only one partition in each curated data product within the gold layer. This will reduce cost and time when recovering from a bad event.

## Pattern

The general pattern for reprocessing data is as follows:

1. Identify the impacted partitions
1. If reprocessing raw events into the silver layer, set job bookmarks as narrowly as possible to reduce the incoming data-set.
1. Take note of the job start time.
1. Re-build the entire target partition
1. Write the data-set to the same location as the target data-set
1. Delete all files in the partition location that existed before the job began

## Pseudo-Code

```Python
job_start = datetime.utcnow()
df = read_input_data(*partition_values)
final_df = build_partition(df)
write_output_data(final_df)
delete_files_before(job_start)
```
