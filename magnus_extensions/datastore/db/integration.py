from magnus.integration import BaseIntegration


class LocalContainerComputeDBStore(BaseIntegration):
    """
    Integration between local container and DB run log store
    """
    executor_type = 'local-container'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'db'  # The actual implementation of the service

    def configure_for_execution(self, **kwargs):
        # https://stackoverflow.com/questions/24319662/
        connection_string = self.service.config['connection_string']
        if 'localhost' in connection_string:
            # TODO this could be buggy and should be revisited later
            connection_string = connection_string.replace('localhost', 'host.docker.internal')
            self.service.config['connection_string'] = connection_string
