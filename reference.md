
## Introduction

AWS Glue provides a serverless runtime for Apache Spark. 
It extends Spark with additional capabilities that are specific to the AWS ecosystem.

The use cases for this service can be broken down into two broad catagories:

The first is someone wanting to do ad-hoc analysis.
Or, a team may need to do some manaual wrangling of data before loading it into a system.
These uses are successful because there is a human that is able to look at the output and use their judgement to modify the code on the fly to produce the desired results.

The other involves using Glue as part of a production data pipeline.
Glue Jobs are expected to run un-attended, reliably, and accuratly.
Error in the output of these jobs can have subtle but catistrophic effects on downstream systems.
Depending on the results, these errors may not be noticed for several days or weeks.
These jobs may also be very complicated and need to handle many different data scenerios.

In this second class of use cases, Glue jobs are no longer one-off scripts, but rather software applications.
Therefore, we need to apply a software engineering process.
This document is our attempt to explain the best practices for how to create maintainable, testable, and scalable Glue jobs for a professional development organization. 
It includes a companion library to help software teams achieve these goals.

This guide assumes the reader has:
- A general understanding of Python
- Familiarity with AWS and the common services (IAM, S3, Cloudformation)
- Created a basic Glue job. Possibly as part of a tutorial
- The concepts of a Continuious Deployment pipeline and why they are used within software development
- Familiarity with automated unit testing
