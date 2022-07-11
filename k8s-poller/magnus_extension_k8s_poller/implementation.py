import logging
import json
from time import time

import re
from kubernetes import client, config

from magnus import defaults
from magnus.executor import BaseExecutor
from magnus import exceptions
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
        config.load_kube_config(config_file=self.config_path)
        return client

    @property
    def polling_time(self):
        """
        Time in seconds to be used for polling k8s job completion
        """
        return self.config.get('polling_time', self.DEFAULT_POLLING_TIME)

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

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        self._submit_k8s_job()
        while self._poll_k8s_job():
            time.time.sleep(self.polling_time)
    
    def _submit_k8s_job(self, node, map_variable:dict, **kwargs):
        #TODO
        """
        "labels": {
            "project": "Dataiku"
        },
        """
        
        command = utils.get_node_execution_command(self, node, map_variable=map_variable)
        logger.info(f'Triggering a batch job with {command}')
        
        mode_config = self.resolve_node_config(node)
        resource_configuration = mode_config.get('resource', None)

        volume_configuration = mode_config.get('volume', None)

        labels = mode_config.get('label', {})
        labels['job_name'] = re.sub('[^A-Za-z0-9]+', '-', f'{self.run_id}-{node.internal_name}')[:63]
        
        image_name = mode_config.get('image_name', None)
        assert image_name is not None, "Complete image_name should be passed for k8s execution"

        k8s_batch = self._client.BatchV1Api()

        base_container = self._client.V1Container(
            name=labels['job_name'],
            image=image_name,
            resources=resource_configuration,
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


    def _poll_k8s_job(self):
        return True

