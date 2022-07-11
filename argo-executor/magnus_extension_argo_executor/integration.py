import logging

from magnus.integration import BaseIntegration
from magnus import defaults

logger = logging.getLogger(defaults.NAME)


class LocalAWSBatchComputeRunLogStoreFileSystem(BaseIntegration):
    """
    Integration between Local AWS batch compute and file-system as run log store
    """
    mode_type = 'local-aws-batch'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        raise Exception(msg)


class LocalAWSBatchComputeRunLogStoreBuffered(BaseIntegration):
    """
    Integration between Local AWS batch compute and file-system as run log store
    """
    mode_type = 'local-aws-batch'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 'buffered'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        raise Exception(msg)


class LocalAWSBatchComputeCatalogFileSystem(BaseIntegration):
    """
    Integration between Local AWS batch compute and file-system as run log store
    """
    mode_type = 'local-aws-batch'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 'file-system'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        raise Exception(msg)


class LocalAWSBatchComputeSecretsDotEnv(BaseIntegration):
    """
    Integration between Local AWS batch compute and file-system as run log store
    """
    mode_type = 'local-aws-batch'
    service_type = 'secrets'  # One of secret, catalog, datastore
    service_provider = 'dotenv'  # The actual implementation of the service

    def validate(self, **kwargs):
        msg = (
            f"Compute mode {self.mode_type} is not compatible with {self.service_provider} for {self.service_type}"
        )
        logger.warning(msg)


class LocalAWSBatchComputeAWSSecrets(BaseIntegration):
    """
    Integration between Local AWS batch compute and AWS secrets
    """
    mode_type = 'local-aws-batch'
    service_type = 'secrets'  # One of secret, catalog, datastore
    service_provider = 'aws-secrets-manager'  # The actual implementation of the service

    def configure_for_execution(self, **kwargs):
        remove_aws_profile = True

        if 'use_profile_in_batch_job' in self.executor.config and \
                self.executor.config['use_profile_in_batch_job'].lower() == 'true':
            remove_aws_profile = False

        if remove_aws_profile:
            self.service.remove_aws_profile()


class LocalAWSBatchComputeS3Catalog(BaseIntegration):
    """
    Integration between Local AWS batch compute and AWS s3
    """
    mode_type = 'local-aws-batch'
    service_type = 'catalog'  # One of secret, catalog, datastore
    service_provider = 's3'  # The actual implementation of the service

    def configure_for_execution(self, **kwargs):
        remove_aws_profile = True

        if 'use_profile_in_batch_job' in self.executor.config and \
                self.executor.config['use_profile_in_batch_job'].lower() == 'true':
            remove_aws_profile = False

        if remove_aws_profile:
            self.service.remove_aws_profile()


class LocalAWSBatchComputeS3Store(BaseIntegration):
    """
    Integration between Local AWS batch compute and AWS s3
    """
    mode_type = 'local-aws-batch'
    service_type = 'run_log_store'  # One of secret, catalog, datastore
    service_provider = 's3'  # The actual implementation of the service

    def configure_for_execution(self, **kwargs):
        remove_aws_profile = True

        if 'use_profile_in_batch_job' in self.executor.config and \
                self.executor.config['use_profile_in_batch_job'].lower() == 'true':
            remove_aws_profile = False

        if remove_aws_profile:
            self.service.remove_aws_profile()
