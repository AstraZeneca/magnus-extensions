# K8s Polling based Execution

This package is an extension to [magnus](https://github.com/AstraZeneca/magnus-core).

## Provides 

Provides functionality to execute a pipeline on K8s cluster.
The jobs would be polled to understand the status and traverse the graph.

## Installation instructions

```pip install magnus_extension_k8s_poller```

## Set up required to use the extension

Kube Configuration file is required to submit the jobs to the K8s cluster.

## Config parameters

The full configuration of the AWS secrets manager is:

```yaml
mode:
  type: k8s-poller
  config:
    config_path: Required and should be pointing to the kube config.
    polling_time: defaults to 30 secs
    secrets_to_use: A list of secrets to use that are part of K8s secrets manager.
    namespace: defaults to "default", the namespace to submit the jobs.
    job_ttl: maximum job run time.
    enable_parallel: Defaults to True, submit parallel jobs to the K8s cluster
    image_name: Required, the full name of the docker image.

```

### **config_path**:

The path of the config file to interact with the K8s cluster.

### **polling_time**:

Defaults to 30 seconds. The frequency to poll k8s to the job status.


### **secrets_to_use**:

A list of secrets to use as part of the Kubernetes cluster.


### **namespace**:

The namespace to submit jobs, defaults to "default".


### **job_ttl**:

The maximum run tile for a job in K8s cluster.


### **enable_parallel**:

Controls if the jobs should be submitted parallelly to the K8s cluster. Default is True.



### **image_name**:

The full name of the docker image to run. Kubernetes cluster should be able to pull the image from the registry.





