# Reprocessing Data

Reprocessing data within a data lake presents certain challenges. If you think of your traditional bronze/silver/gold data lake architecture, this will only apply to the silver and gold layers. The bronze layer should never be modified to allow re-creation of the more curated levels.

## Pattern

The general pattern for reprocessing data is as follows:

1. Identify the impacted partitions
1. If reprocessing raw events into the silver layer, set job bookmarks as narrowly as possible to reduce the incoming data-set.
1. Take note of the job start time.
1. Re-build the entire target partition
1. Write the data-set to the same location as the target data-set
1. Delete all files in the partition location that existed before the job began

### Pseudo-Code

```Python
job_start = datetime.utcnow()
df = read_input_data(*partition_values)
final_df = build_partition(df)
write_output_data(final_df)
delete_files_before(job_start)
```
### Partitioning

Care should be taken when designing the data lake to design partitions that limit the blast radius of the reprocessing of the reprocessing of any partitions. Analyze access patterns and potential damage a malformed event could cause when processed by the data lake. A good rule of thumb is that a bad event should only cause the reprocessing of a single partition in the silver layer, and only one partition in each curated data product within the gold layer. This will reduce cost and time when recovering from a bad event.

## Idempotency

Idempotency, or the ability to apply a process multiple times without changing the end state, is the end goal of our strategy. This concept is particularly important when creating a data lake. Having all operations be idempotent allows us to re-build the lake from the raw events by merely re-running our jobs. 

With relation to partitions, this is important because it allows us to re-build a partition by just re-running a single job, instead of performing data surgery for hours beforehand.

## Bronze/Silver/Gold Architecture

The Bronze/Silver/Gold architecture is a common pattern for data lakes. In it, we model our data in a series of layers, starting with the low quality data at one end (Bronze), and use ETL jobs to progressively enhance the data as it moves between layers (Silver) until we have a quality data product that is ready for end user interaction (Gold).

For example, we may have a raw series of events landed in an S3 bucket. This would be our bronze layer. We might use a series of glue jobs to remove nulls and separate the raw events into conceptual tables in the silver layer. Finally another series of jobs would read data from the silver layer and create a data product that is useful for our ultimate consumers in the gold layer. This final layer would be the part that our users would interact with.

Despite the name, this architecture should not limit you to three layers. There can be as many intermediate layers as your data processing needs require.
