## Data Lake Data Model

A common use for AWS Glue is to process data within a Data Lake environment.
In these architectures data is typically stored in S3 as Parquet files.
For teams that are used to working with the Dimensional or Snowflakes of a data warehouse, they may be tempted to create a similar data structure in the Data Lake. 
This would take the form of a table within the Glue catalog for each entity and relationship.
We recommend against this.

The constraints within a S3, Parquet based lake are not the same as those on a relational database.
- Joins are expensive
- Individual rows cannot be updated. Instead, entire partitions are rewritten or appended to.
- A large amount of data can be scaned in one pass.
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
So if a policy holder made 100 pyaments, then yes, the name, address, phone number, etc would be duplicated 100 times.

There may also be source data where there are multiple values per row.
If this is the case (like a `label` field that could be 0 - many strings) then the team should consider the various data structure types supported by Parquet as well as Glue and Athena. 
Unlike a relational database, there is no indexing advantage to putting a list of strings in a separate table, instead, if it is stored as an `array<string>` column then it can be queried just like any other column and the user is not forced to join it to a parent table to produce useful data.

The kneejerk reaction to this is that it is a "bad" design because of all this duplication and that it would cause a lot of wasted space, performance problems, data correctness issues, etc.
Infact, this design makes the Data Lake both simpler to build and faster to use. 
- Parquet will automatically deduplicate column values within itself. 
    This means that it doesn't matter if we store the same name 100s of times, the actual space will be far less. 
- This "wide table" strategy also simplifies the process of loading data: 
    Rather than having a bunch of separate jobs for each of several tables, One job can be created that joins the data together and loads the single table.
- Data is easier to reason about:
    Users don't have to know what columns to join on and make sure join criteria are correct to prevent issues.
    They can simply query the table and group or filter by any available columns.
- With mutliple table, some data could be loaded into one table while other tables have not yet been loaded or their loads failed.

There is a limit to this setup. 
The table can have hundreds, but probably not thousands of columns. 
There should be some consideration for the users and use cases for the data. 
Where a traditional Data Warehouse has "Data Marts" that are copies of data for different audiances, a Data Lake can also have different tables that are focused on different use cases where needed. 
This does not remove the need to sometimes have other tables at different grains.
In our example there is still probably going to be a `PolicyHolders` table that has one row per customer.
An important distinction is that these two tables are fully independent and not tied together like in a Dimensional or Snowflake design.

