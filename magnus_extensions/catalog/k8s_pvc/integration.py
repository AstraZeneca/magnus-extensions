import logging

from magnus import defaults
from magnus.integration import BaseIntegration

logger = logging.getLogger(defaults.NAME)


class LocalComputeK8sPVCCatalog(BaseIntegration):
    """
    Integration between local and k8's pvc
    """
    mode_type = 'local'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'k8s-pvc'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            "We can't use the local compute k8s pvc store integration."
        )
        raise Exception(msg)


class LocalContainerComputeK8sPVCCatalog(BaseIntegration):
    """
    Integration between local-container and k8's pvc
    """
    mode_type = 'local-container'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'k8s-pvc'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            "We can't use the local-container compute k8s pvc store integration."
        )
        raise Exception(msg)


class KfpComputeK8sPVCCatalog(BaseIntegration):
    """
    Integration between kfp and k8's pvc
    """
    mode_type = 'kfp'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'k8s-pvc'  # The actual implementation of the service

    def configure_for_traversal(self, **kwargs):
        self.executor.persistent_volumes["catalog"] = (self.service.persistent_volume_name, self.service.mount_path)


class ArgoComputeK8sPVCCatalog(BaseIntegration):
    """
    Integration between argo and k8's pvc
    """
    mode_type = 'argo'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'k8s-pvc'  # The actual implementation of the service

    def configure_for_traversal(self, **kwargs):
        self.executor.persistent_volumes["catalog"] = (self.service.persistent_volume_name, self.service.mount_path)
