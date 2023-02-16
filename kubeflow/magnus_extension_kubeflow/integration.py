from magnus.integration import BaseIntegration


class KfPComputeBufferedRunLogStore(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """
    mode_type = 'kfp'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'buffered'  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception('KFP cannot run work with buffered run log store')


class KfPComputeFileSystemRunLogStore(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """
    mode_type = 'kfp'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception('KFP cannot run work with file system as run log store')


class KfPComputeFileSystemCatalog(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """
    mode_type = 'kfp'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception('KFP cannot run work with file system catalog')
