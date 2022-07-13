from magnus.integration import BaseIntegration


class K8sPollerComputeRunLogStoreBuffered(BaseIntegration):
    """
    Integration between k8s-poller and Buffered as run log store
    """
    mode_type = 'k8s-poller'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'buffered'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        raise Exception(msg)


class K8sPollerComputeRunLogStoreFileSystem(BaseIntegration):
    """
    Integration between k8s-poller and file-system as run log store
    """
    mode_type = 'k8s-poller'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        raise Exception(msg)


class K8sPollerComputeCatalogFileSystem(BaseIntegration):
    """
    Integration between k8s-poller and file-system as catalog
    """
    mode_type = 'k8s-poller'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        raise Exception(msg)
