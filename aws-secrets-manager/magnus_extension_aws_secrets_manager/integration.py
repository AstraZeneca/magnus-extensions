from pathlib import Path

from magnus.integration import BaseIntegration


class LocalContainerComputeAWSSecrets(BaseIntegration):
    """
    Integration between local container and AWS secrets
    """
    mode_type = 'local-container'
    service_type = 'secrets'  # One of secret, catalog, datastore
    service_provider = 'aws-secrets-manager'  # The actual implementation of the service

    def configure_for_traversal(self, **kwargs):
        write_to = self.service.get_aws_credentials_file()
        self.executor.volumes[str(Path(write_to).resolve())] = {
            'bind': '/root/.aws/credentials',
            'mode': 'ro'
        }
