import logging

from magnus import defaults
from magnus.integration import BaseIntegration

logger = logging.getLogger(defaults.NAME)


class LocalComputeChunkedK8sPVCStore(BaseIntegration):
    """
    Integration between local and k8's pvc
    """
    mode_type = 'local'
    service_type = 'run-log-store'  # One of secret, catalog, datastore
    service_provider = 'chunked-k8s-pvc'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            "We can't use the local compute k8s pvc store integration."
        )
        raise Exception(msg)


class LocalContainerComputeChunkedK8sPVCStore(BaseIntegration):
    """
    Integration between local-container and k8's pvc
    """
    mode_type = 'local-container'
    service_type = 'run-log-store'  # One of secret, catalog, datastore
    service_provider = 'chunked-k8s-pvc'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            "We can't use the local-container compute k8s pvc store integration."
        )
        raise Exception(msg)


class KfpComputeChunkedK8sPVCStore(BaseIntegration):
    """
    Integration between kfp and k8's pvc
    """
    mode_type = 'kfp'
    service_type = 'run-log-store'  # One of secret, catalog, datastore
    service_provider = 'chunked-k8s-pvc'  # The actual implementation of the service

    def configure_for_traversal(self, **kwargs):
        self.executor.persistent_volumes["run_log_store"] = (
            self.service.persistent_volume_name, self.service.mount_path)


class ArgoComputeChunkedK8sPVCStore(BaseIntegration):
    """
    Integration between argo and k8's pvc
    """
    mode_type = 'argo'
    service_type = 'run-log-store'  # One of secret, catalog, datastore
    service_provider = 'chunked-k8s-pvc'  # The actual implementation of the service

    def configure_for_traversal(self, **kwargs):
        self.executor.persistent_volumes["run_log_store"] = (
            self.service.persistent_volume_name, self.service.mount_path)
