import json
import logging
from functools import lru_cache

from magnus import defaults, exceptions
from magnus.secrets import BaseSecrets

logger = logging.getLogger(defaults.NAME)

try:
    from magnus_extensions.aws import AWSConfigMixin
except ImportError as _e:  # pragma: no cover
    msg = (
        "AWS Dependencies are not installed!!"
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

    class Config(AWSConfigMixin.Config):
        secret_arn: str
        secret_version_id: str = ""

    def __init__(self, config: dict, **kwargs):
        super().__init__(config, **kwargs)
        self.secrets = {}

    @property
    def secret_arn(self) -> str:
        """Returns the secret arn stored in the config

        Returns:
            str: The arn of the secret
        """
        return self.config.secret_arn

    @property
    def secret_version(self) -> str:
        """
        Returns the version ID if provided in the config

        Returns:
            str: The version ID as specified in the config or None
        """
        return self.config.secret_version_id

    @lru_cache(maxsize=2)
    def load_secrets(self):
        """
        One time load of all the secrets
        """
        boto_session = self.get_boto3_session()

        client = boto_session.client('secretsmanager')
        if self.secret_version:
            response = client.get_secret_value(SecretId=self.secret_arn, VersionId=self.secret_version)
        else:
            response = client.get_secret_value(SecretId=self.secret_arn)
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
