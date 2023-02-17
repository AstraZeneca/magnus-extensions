from pathlib import Path

from magnus.integration import BaseIntegration


class LocalContainerComputeS3Catalog(BaseIntegration):
    """
    Integration pattern between Local container and S3  catalog
    """
    mode_type = 'local-container'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 's3'  # The actual implementation of the service

    def configure_for_traversal(self, **kwargs):
        write_to = self.service.get_aws_credentials_file()
        self.executor.volumes[str(Path(write_to).resolve())] = {
            'bind': '/root/.aws/credentials',
            'mode': 'ro'
        }
