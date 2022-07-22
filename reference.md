
## Introduction

AWS Glue is a powerful service that brings a "serverless" mindset to batch data processing.
Spark is tricky though.
When an engineer begins looking at using Glue they will ineviatbly look at some of the Glue example jobs.
These jobs can trick the reader into thinking that they are resilent to common production scenerios and will work as advertised every night they run. 
In reality, they assume the most "happy path" situation where no job fails, data is always present and automated tests are not desired.

Many use cases for Spark are similar to those for SQL. 
Sometimes somone just needs to do some ad-hoc analysis.
Or, a team may need to do some manaual wrangling of data before loading it into a system.
These uses are successful because there is a human that is able to look at the output and use their judgement to modify the code on the fly to produce the desired results.

This document is not for these use cases. 
Instead, it is for a second class of users.
These users are looking to use Glue and Spark in a "productionalized" environment to replace scheduled batch jobs.
These jobs often run un-attended.
Error in the output of these jobs can have subtle but catistrophic effects on downstream systems.
Depending on the results, these errors may not be noticed for several days or weeks.
These jobs may also be very complicated and need to handle many different data scenerios.

In this second class of use cases, Glue jobs are no longer one-off scripts, but rather software applications.
Therefore, we need to apply a software development process.
This document is our attempt to explain the best practices for how to create maintainable, testable, and scalable Glue jobs for a professional development organization. 
It includes a companion library to help to aleviate some of the boilerplate needed in order to achieve these goals.