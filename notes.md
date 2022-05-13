
## 3 outputs/artifacts:
    - Document about how to "do glue right"
        - What "problems" should this document address?
    - Library that assists with the doucment (boilerplate)
    - Example using the above

## Questions:
    - Do we focus on Glue, or support PySpark Vanilla (all in glue)

## Outline

1. Introduction
    1. Target audiance: Enterprise development teams
    1. What problem does this solve
    1. This approach
1. Glue Job Structure
1. Testing
1. Schema Management
    1. Why a schema
        1. Acts as the "API" between jobs and processes
    1. Schema Sources
        1. Derived from the data itself
            1.  Glue Crawlers
        1. Glue Table definition
        1. Avro
        1. Pyspark Schema
    1. Ensuring compliance with a schema
1. Partitioning

