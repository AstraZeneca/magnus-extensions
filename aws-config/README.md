# Magnus AWS Configuration Mixin

This package provides the general AWS configuration methods used by services based on AWS.

This package is not meant to be used by its own but designed to be used as a dependency for actual AWS 
service providers.

## Installation instructions

```pip install magnus_extension_aws_config```

## Set up required to use the extension

Access to AWS environment either via:

- AWS profile, generally stored in ~/.aws/credentials
- AWS credentials available as environment variables

If you are using environmental variables for AWS credentials, please set:

- AWS_ACCESS_KEY_ID: AWS access key
- AWS_SECRET_ACCESS_KEY: AWS access secret
- AWS_SESSION_TOKEN: The session token, useful to assume other roles # Optional


## Config parameters

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

**If a ```role_arn``` is provided, we try to get a session against that, otherwise it will just use the access keys**
