import logging
from pathlib import Path

from magnus import defaults
from magnus.integration import BaseIntegration

logger = logging.getLogger(defaults.NAME)


class LocalContainerComputeS3Store(BaseIntegration):
    """
    Integration between local container and S3 run log store
    """
    mode_type = 'local-container'
    service_type = 'run-log-store'  # One of secret, catalog, datastore
    service_provider = 'chunked-s3'  # The actual implementation of the service

    def validate(self, **kwargs):
        pass

    def configure_for_traversal(self, **kwargs):
        write_to = self.service.get_aws_credentials_file()
        self.executor.volumes[str(Path(write_to).resolve())] = {
            'bind': '/root/.aws/credentials',
            'mode': 'ro'
        }
