
import logging
import re
import shlex
from typing import List

from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph
from magnus.nodes import BaseNode
from ruamel.yaml import YAML

logger = logging.getLogger(defaults.NAME)

try:
    from kubernetes import client
    from kubernetes import config as k8s_config
    from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector
except ImportError as _e:
    msg = (
        "Kuberneters Dependencies have not been installed!!"
    )
    raise Exception(msg) from _e


def secret_to_env_var(secret_name: str, secret_key: str, env_var_name: str) -> V1EnvVar:
    """Turns a K8s secret into an environment variable for use in a kubeflow component"""
    return V1EnvVar(
        name=env_var_name,
        value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name=secret_name, key=secret_key)),
    )


class KubeFlowExecutor(BaseExecutor):

    service_name = "k8s-job"

    class Config(BaseExecutor.Config):
        config_path: str
        docker_image: str
        namespace: str = "default"
        cpu_limit: str = "250m"
        memory_limit: str = "1G"
        gpu_limit: int = 0
        cpu_request: str = ""
        memory_request: str = ""
        active_deadline_seconds: int = 60 * 60 * 2  # 2 hours
        ttl_seconds_after_finished: int = 60  #  1 minute
        image_pull_policy: str = "Always"
        secrets_from_k8s: dict = {}  # EnvVar=SecretName:Key
        persistent_volumes: dict = {}  # volume-name:mount_path
        labels: dict[str, str] = {}

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.persistent_volumes = {}

        for volume_name, mount_path in self.config.persistent_volumes.items():
            self.persistent_volumes["executor"] = (volume_name, mount_path)

    def prepare_for_graph_execution(self):
        """
        This method would be called prior to calling execute_graph.
        Perform any steps required before doing the graph execution.

        The most common implementation is to prepare a run log for the run if the run uses local interactive compute.

        But in cases of actual rendering the job specs (eg: AWS step functions, K8's) we need not do anything.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_traversal(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_traversal(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_traversal(self, self.secrets_handler)

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_traversal(self, self.experiment_tracker)

    def prepare_for_node_execution(self):
        """
        Perform any modifications to the services prior to execution of the node.

        Args:
            node (Node): [description]
            map_variable (dict, optional): [description]. Defaults to None.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_execution(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_execution(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_execution(self, self.secrets_handler)

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_execution(self, self.experiment_tracker)

        self._set_up_run_log(exists_ok=True)

    def _client(self):
        k8s_config.load_kube_config(config_file=self.config.config_path)
        return client

    def execute_job(self, node: BaseNode):
        command = utils.get_job_execution_command(self, node)
        logger.info(f'Triggering a kubernetes job with : {command}')

        self.config.labels['job_name'] = self.run_id

        k8s_batch = self._client.BatchV1Api()

        cpu_limit = self.config.cpu_limit
        memory_limit = self.config.memory_limit

        cpu_request = self.config.cpu_request or cpu_limit
        memory_request = self.config.memory_request or memory_limit

        gpu_limit = self.config.gpu_limit

        limits = {"cpu": cpu_limit, "memory": memory_limit, "gpu": gpu_limit}
        requests = {"cpu": cpu_request, "memory": memory_request}
        resources = {"limits": limits, "requests": requests}

        secret_configuration = []
        for secret_env, k8_secret in self.config.secrets_from_k8s.items():
            try:
                secret_name, key = k8_secret.split(':')
            except Exception as _e:
                msg = (
                    "K8's secret should be of format EnvVar=SecretName:Key"
                )
                raise Exception(msg) from _e
            secret_configuration.append(secret_to_env_var(secret_name=secret_name,
                                                          secret_key=key, env_var_name=secret_env))

        pod_volumes = []
        volume_mounts = []
        for i, (volume_name, mount_path) in enumerate(self.persistent_volumes.items()):
            pod_volumes.append(self._client.V1Volume(name=f"magnus-executor-{i}", persistent_volume_claim=volume_name))
            volume_mounts.append(self._client.V1VolumeMount(name=f"magnus-executor-{i}", mount_path=mount_path))

        base_container = self._client.V1Container(
            name=self.run_id,
            image=self.config.docker_image,
            command=shlex.split(command),
            resources=resources,
            env_from=secret_configuration,
            image_pull_policy="Always",
            volume_mounts=volume_mounts or None
        )

        pod_spec = self._client.V1PodSpec(volumes=pod_volumes or None,
                                          restart_policy='Never',
                                          containers=[base_container])

        pod_template = self._client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=self.config.labels), spec=pod_spec)

        job_spec = client.V1JobSpec(template=pod_template, backoff_limit=2)
        job_spec.active_deadline_seconds = self.config.active_deadline_seconds

        job = client.V1Job(
            api_version='batch/v1',
            kind='Job',
            metadata=client.V1ObjectMeta(name=self.run_id),
            spec=job_spec)

        k8s_batch.create_namespaced_job(body=job, namespace=self.namespace)

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        step_log = self.run_log_store.create_step_log(node.name, node._get_step_log_name(map_variable))

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        step_log = self.run_log_store.get_step_log(node._get_step_log_name(map_variable), self.run_id)
        if step_log.status == defaults.FAIL:
            raise Exception(f'Step {node.name} failed')

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        msg = (
            "This executor is not supported to execute any graphs but only jobs (functions or notebooks)"
        )
        raise NotImplementedError(msg)

    def send_return_code(self, stage='traversal'):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        if stage != 'traversal':  # traversal does no actual execution, so return code is pointless
            run_id = self.run_id

            run_log = self.run_log_store.get_run_log_by_id(run_id=run_id, full=False)
            if run_log.status == defaults.FAIL:
                raise Exception('Pipeline execution failed')
