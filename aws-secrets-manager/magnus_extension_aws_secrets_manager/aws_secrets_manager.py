import logging
from functools import lru_cache
import json

from magnus import defaults
from magnus.secrets import BaseSecrets
from magnus import exceptions


logger = logging.getLogger(defaults.NAME)

try:
    from magnus_extension_aws_config.aws import AWSConfigMixin
except ImportError as _e:  # pragma: no cover
    msg = (
        'Please install magnus_extension_aws_config which provides the general utilities for AWS services.'
    )
    raise Exception(msg) from _e


class AWSSecretsManager(BaseSecrets, AWSConfigMixin):
    """
    Implementation of secrets manager of AWS

    Example config:
    secrets:
      type: 'aws-secrets-manager'
      config:
        secret_arn: The secret ARN to retrieve the secrets from.
        region: The aws region to use
        aws_profile: The aws profile to use. The executor which uses the secrets manager can also control
                this behavior by the integration pattern.
    """
    service_name = 'aws-secrets-manager'

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        if not self.config:
            raise Exception('A config containing the secret_arn should be present for AWS secrets manager')
        if 'secret_arn' not in self.config:
            raise Exception('The Secret ARN should be specified in secret_arn for AWS secrets Manager')
        self.secrets = {}

    def get_secret_arn(self) -> str:
        """Returns the secret arn stored in the config

        Returns:
            str: The arn of the secret
        """
        return self.config.get('secret_arn')

    def get_version_id(self) -> str:
        """
        Returns the version ID if provided in the config

        Returns:
            str: The version ID as specified in the config or None
        """
        return self.config.get('secret_version_id', None)

    @lru_cache(maxsize=2)
    def load_secrets(self):
        """
        One time load of all the secrets
        """
        boto_session = self.get_boto3_session()

        client = boto_session.client('secretsmanager')
        if self.get_version_id():
            response = client.get_secret_value(SecretId=self.get_secret_arn(), VersionId=self.get_version_id())
        else:
            response = client.get_secret_value(SecretId=self.get_secret_arn())
        self.secrets = json.loads(response['SecretString'])

    def get(self, name: str = None, **kwargs):
        """
        Return the secret by name.
        If no name is give, return all the secrets.

        Args:
            name (str): The name of the secret to return.
        """
        self.load_secrets()
        if not name:
            return self.secrets

        if name in self.secrets:
            return self.secrets[name]

        secrets_location = self.get_secret_arn()
        raise exceptions.SecretNotFoundError(secret_name=name, secret_setting=secrets_location)
