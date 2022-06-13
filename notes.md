
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
    1.  Unit Testing
        1. Focuses on logic/business rules
        1. Spark "in the loop"
        1. Mock external IO (ex. Calls to s3, jdbc, etc)
        1. How do we create and manage test data
    1. Smoke/Integration/E2E tests
        1. Really running the job in AWS
        1. Focus on infrastructure, security, config, etc
        1. Validates some of the assumptions when unit testing
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


## TODOS:
- Hack together the sample job to just get it working locally in container
- Define the example job output schema
- Take a first cut at the "job structure" question
- Redo the job with TDD to validate testing strategy
- 