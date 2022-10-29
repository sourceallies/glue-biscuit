
## Consuming Event/Change Records

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
- Opitonally, to support deletes, each record needs a way to identify that it is a delete.

This is the logical algorithm to process this data:
1. New records are loaded from the source (usually via a Bookmark)
1. Source events are converted into the target structure. As part of this conversion, populate an `effective_date` column with the event time and a `_deleted` flag with `true` if the event represents a deletion and `false` otherwise.
1. Load the current target dataset.
1. Add a `_deleted` column to the target with a value of `false`
1. Union the source and target
1. Execute a group-by on the primary key columns.
1. For each group, reduce the group by selecting the record with the largest timestamp (most current record). 
1. This resulting dataset now has exactly one record per entity
1. Filter out any records with a `_deleted` flag that is `true`
1. Drop the `_deleted` column.
1. Write out the output to the target.
1. Purge data from the target that existed prior to the job run.

In order to help with the execution of the above algorithm, we provide a [merge_and_retain_last](src/framework/merge_utils.py) function to help with the middle steps between adding a `_deleted` column and dropping it. 