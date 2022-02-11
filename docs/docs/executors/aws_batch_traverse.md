# AWS Batch Traverse

This package is an extension to [magnus](https://github.com/AstraZeneca/magnus-core).

## Provides 
A self-traversing or decentralized executor to off-load individual steps in AWS using Batch.

Decentralized executions is a unique mode in magnus where there is no central server orchestrating the jobs. This allows
for multiple definitions of the pipeline to exist at the same time without any conflicts to other members of the same
project. 

This mode is ideal for data science experimentation where all data scientists of a team are free to experiment and 
use AWS cloud resources.

## Installation instructions

```pip install magnus_extension_executor_aws_batch_traverse```

## Set up required to use the extension

An AWS environment with the following services:

- A ECR to push docker images.
- A batch compute environment with necessary compute for your jobs
- A job queue to queue jobs for compute.

Since the AWS batch job spins the subsequent jobs of the dag as part of its job execution, the execution role of the 
batch should have necessary privileges to trigger batch jobs.

## Config parameters

The full configuration to use this compute is below:

```yaml
mode:
  type: local-aws-batch
  config:
    aws_batch_job_definition: #The batch job definition to use, required
    aws_batch_job_queue: #The batch job queue to send the jobs, required
    region: #The aws region to use, defaults to eu-west-1
    tags: # a mapping, defaults to None
    aws_profile: #The aws profile to use to submit the batch jobs, defaults to default

```

### **aws_batch_job_definition**:

The ```arn``` or the name of the AWS batch job definition to use for the batch job submission. The format is 
the same as 
[AWS Submit Batch job](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.submit_job) 


### **aws_batch_job_queue**:

The ```arn``` or the name of the AWS job queue to que the job. The format is the same as
[AWS Submit Batch job](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.submit_job) 


### **tags**:

Any optional tags that you want to assign to the batch job. The format is a mapping that is submitted to the batch job.

The below parameters are inherited from AWS Configuration.

### **aws_profile**:

The profile to use for acquiring boto3 sessions. 

Defaults to None, which is used if its role based access or in case of credentials present as environmental variables.

### **region**:

The region to use for acquiring boto3 sessions.

Defaults to *eu-west-1*.


### **aws_credentials_file**:

The file containing the aws credentials.

Defaults to ```~/.aws/credentials```.

### **use_credentials**:

Set it to ```True``` to provide AWS credentials via environmental variables.

Defaults to ```False```.

### ***role_arn***:

The role to assume after getting the boto3 session.

**This is required if you are using ```use_credentials```.**

