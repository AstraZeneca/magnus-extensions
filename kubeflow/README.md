# AWS Secrets manager

This package is an extension to [magnus](https://github.com/AstraZeneca/magnus-core).

## Provides 

Provides functionality to use Kubeflow pipelines as an Executor

## Installation instructions

```pip install magnus_extension_kubeflow```

## Set up required to use the extension


## Config parameters

The full configuration of the AWS secrets manager is:

```yaml
mode:
  type: 'kfp'
  config:
    docker_image: # Required
    output_file: 'pipeline.yaml'
    default_cpu_limit: "250m"
    default_memory_limit: "1G"
    default_cpu_request:  "" # defaults to default_cpu_limit
    default_memory_request: "" # defaults to default_memory_limit
    enable_caching: False
    image_pull_policy: "IfNotPresent"
    secrets_from_k8s: null # dictionary of EnvVar=SecretName:Key
```

