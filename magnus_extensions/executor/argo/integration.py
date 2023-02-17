from magnus.integration import BaseIntegration


class ArgoComputeBufferedRunLogStore(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """
    mode_type = 'argo'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'buffered'  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception('Argo cannot run work with buffered run log store')


class ArgoComputeFileSystemRunLogStore(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """
    mode_type = 'argo'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception('Argo cannot run work with file system as run log store')


class ArgoComputeFileSystemCatalog(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """
    mode_type = 'argo'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception('Argo cannot run work with file system catalog')
