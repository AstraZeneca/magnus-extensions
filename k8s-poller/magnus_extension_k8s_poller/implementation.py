import logging
from tqdm import tqdm
import time
import shlex
import re
from kubernetes import client, k8s_config

from magnus import defaults
from magnus.executor import BaseExecutor
from magnus.nodes import BaseNode
from magnus import utils


logger = logging.getLogger(defaults.NAME)


class K8sExecutor(BaseExecutor):
    service_name = 'k8s-poller'
    DEFAULT_POLLING_TIME = 30
    DEFAULT_JOB_TTL = 10000
    DEFAULT_KUBE_NAMESPACE = "default"

    def __init__(self, config):
        super().__init__(config)
        assert 'config_path' in config, "config_path is required for k8s execution"

    @property
    def config_path(self):
        return self.config['config_path']

    @property
    def _client(self):
        k8s_config.load_kube_config(config_file=self.config_path)
        return client

    @property
    def polling_time(self):
        """
        Time in seconds to be used for polling k8s job completion
        """
        return self.config.get('polling_time', self.DEFAULT_POLLING_TIME)

    @property
    def secrets_to_use(self):
        """
        Time in seconds to be used for polling k8s job completion
        """
        return self.config.get('secrets_to_use', [])

    @property
    def namespace(self):
        """
        K8s namespace to be used for execution
        """
        return self.config.get('namespace', self.DEFAULT_KUBE_NAMESPACE)

    @property
    def job_ttl(self):
        """
        Max completion Time in seconds for k8s job
        """
        return self.config.get('job_ttl', self.DEFAULT_JOB_TTL)

    def is_parallel_execution(self):
        if self.config and 'enable_parallel' in self.config:
            return self.config.get('enable_parallel').lower() == 'true'

        return True

    def get_job_name(self, node: BaseNode, map_variable: dict) -> str:
        """
        Generate a job name based on the node being executed.

        Args:
            node (BaseNode): The node being executed
            map_variable (dict): The map variable if running as a map node

        Returns:
            str: The job name, maximum size of 63 characters
        """
        resolved_name = node.resolve_map_placeholders(name=node.internal_name, map_variable=map_variable)
        return re.sub('[^A-Za-z0-9]+', '-', f'{self.run_id}-{resolved_name}')[:63]

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        self._submit_k8s_job(node=node, map_variable=map_variable)

        while self._poll_k8s_job(node=node, map_variable=map_variable):
            for _ in tqdm(range(self.polling_time), desc="waiting..."):
                time.sleep(1)

    def _submit_k8s_job(self, node: BaseNode, map_variable: dict, **kwargs):  # pylint: disable=unused-argument
        """
        Submit a job to the K8's cluster

        Args:
            node (BaseNode): The node being processed
            map_variable (dict): The map variables if the node is part of a map
        """

        command = utils.get_node_execution_command(self, node, map_variable=map_variable)
        logger.info(f'Triggering a batch job with {command}')

        mode_config = self.resolve_node_config(node)

        image_name = mode_config.get('image_name', None)
        assert image_name is not None, "Complete image_name should be passed for k8s execution"

        resource_configuration = mode_config.get('resource', None)
        # volume_configuration = mode_config.get('volume', None) # TODO Should this also be a list?

        labels = mode_config.get('labels', {})
        labels['job_name'] = self.get_job_name(node=node, map_variable=map_variable)

        k8s_batch = self._client.BatchV1Api()

        secret_configuration = None
        if self.secrets_to_use:
            secret_configuration = []
            for secret_name in self.secrets_to_use:
                k8s_secret_env_source = self._client.V1SecretEnvSource(name=secret_name)
                secret_configuration.append(self._client.V1EnvFromSource(secret_ref=k8s_secret_env_source))

        base_container = self._client.V1Container(
            name=labels['job_name'],
            image=image_name,
            command=shlex.split(command),
            resources=resource_configuration,
            env_from=secret_configuration,
            image_pull_policy="Always"
        )

        pod_volume_template = None
        # if volume_configuration:
        #     pod_volume_template = self.create_pod_volume_template()
        pod_spec = self._client.V1PodSpec(volumes=pod_volume_template,
                                          restart_policy='Never',
                                          containers=[base_container])

        pod_template = self._client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels),
            spec=pod_spec)

        job_spec = client.V1JobSpec(template=pod_template, backoff_limit=2)

        job_spec.ttl_seconds_after_finished = 2 * self.polling_time

        job_spec.active_deadline_seconds = self.job_ttl

        job = client.V1Job(
            api_version='batch/v1',
            kind='Job',
            metadata=client.V1ObjectMeta(name=labels['job_name']),
            spec=job_spec)

        k8s_batch.create_namespaced_job(
            body=job,
            namespace=self.namespace)

    def _poll_k8s_job(self, node: BaseNode, map_variable: dict) -> bool:
        """
        Poll the K8's to understand the job status.

        The job could be in one of the status:
        1). Success: The job succeeded and therefore no polling is required any more. Return False
        2). Fail: The job failed and therefore no polling is required any more. Return False
        3). Active: The job is being processed and further polling is required. Return True
        4). Job queued: The job is yet to start and further polling is required. Return True.

        Args:
            node (BaseNode): The node being processed
            map_variable (dict): The map variables if its part of the map node.

        Returns:
            bool: True if the job is processing or yet to be processed. False if its failed or succeeded.
        """
        k8s_batch = self._client.BatchV1Api()
        job_name = self.get_job_name(node=node, map_variable=map_variable)
        job_label = f"job_name={job_name}"

        if self.namespace:
            logger.info(f"Looking for job status in k8s {self.namespace} namespace with label: {job_label}")
            job_status = k8s_batch.list_namespaced_job(namespace=self.namespace, watch=False,
                                                       label_selector=f'{job_label}')
        else:
            # TODO: Does this happen in our case?
            logger.info(f"Looking for job status in k8s with label: {job_label}")
            job_status = k8s_batch.list_job_for_all_namespaces(watch=False,
                                                               label_selector=f'{job_label}')

        namespace_info = "All" if self.namespace is None else self.namespace  # TODO: Does this happen to us?
        assert len(
            job_status.items) == 1, f"Either no status or more than one status returned for Job {job_name} in {namespace_info}"
        # TODO: Can the decision be taken by individual items?
        for i in job_status.items:

            if i.status.succeeded is not None and i.status.succeeded > 0:
                logger.info(f"Job {job_name} in {namespace_info} namespace(s) is completed with status as SUCCESS")
                return False
            elif i.status.failed is not None and i.status.failed > 0:
                logger.info(f"Job {job_name} in {namespace_info} namespace(s) is completed with status as FAILED")
                return False
            elif i.status.active is not None and i.status.active > 0:
                logger.info(f"Job {job_name} in {namespace_info} namespace(s) is RUNNING")
                return True
            else:
                logger.info(f"Job {job_name} is yet to be started in {namespace_info} namespace(s)..")

        return True
