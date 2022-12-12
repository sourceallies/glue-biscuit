# Supporting "Point in Time" Queries

Many times, users have the need to query data as it existed historically.
In traditional data warehoused, this was often implemetned as "effectve" and "term" dates. 
These dates were added to `where` clauses in order to restrict the rows returned to only those effective at the desired time.
When loading data, existing records are "termed" and records with the current state are inserted with new effective dates.

Within a data lake that is backed by files in S3, this architecture creates two problems:
During a load process, we cannot simply update individual records in S3, instead, entire files must be re-written.
Since AWS Glue and other technologies that read from the data lake do not support indexes in the traditional sense, they cannot effectivly filter out records that are not current without scanning all the data in the lake. 

Rather than using effective and term dates, we can instead decide on a "granularity" and then create a partition for each value of that granularity.
For example, if we need to query data by month, we would create a partition for each month and load an entire copy of the data into this partition. 
This creates a copy of the entire dataset every month, but since Parquet files are highly space-efficent and S3 storage is very cheap this is not a concern.
When users want to run a point in time query, they simply specify the partion for the month of conern.

