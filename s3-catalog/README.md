# S3 catalog provider

This package is an extension to [magnus](https://github.com/AstraZeneca/magnus-core).

## Provides 
Provides functionality to use S3 buckets as catalog provider.

## Installation instructions

```pip install magnus_extension_catalog_s3```

## Set up required to use the extension

Access to AWS environment either via:

- AWS profile, generally stored in ~/.aws/credentials
- AWS credentials available as environment variables

If you are using environmental variables for AWS credentials, please set:

- AWS_ACCESS_KEY_ID: AWS access key
- AWS_SECRET_ACCESS_KEY: AWS access secret
- AWS_SESSION_TOKEN: The session token, useful to assume other roles

A S3 bucket that you want to use to store the catalog objects.

## Config parameters

The full configuration required to use S3 as catalog is:

```yaml
catalog:
  type: s3
  config:
    compute_data_folder : # defaults to data/
    s3_bucket: # Bucket name, required
    region: # Region if we are using
    aws_profile: #The profile to use or default
    use_credentials: # Defaults to False

```

### **compute_data_folder**:

The ```data``` folder that is used for your work. 

Logically cataloging works as follows:

- get files from the catalog before the execution to a specific compute data folder
- execute the command
- put the files from the compute data folder to the catalog.


You can over-ride the compute data folder, defined globally, for individual steps by providing it in the step 
configuration.

For example:

```yaml
catalog:
  ...

dag:
  steps:
    step name:
      ...
      catalog:
        compute_data_folder: # optional and only apples to this step
        get:
          - list
        put:
          - list

    ...
```

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
