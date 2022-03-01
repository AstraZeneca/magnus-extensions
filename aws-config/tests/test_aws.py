import pytest

from magnus_extension_aws_config import aws


def test_aws_config_mixin_inits_to_empty_dict():
    aws_config = aws.AWSConfigMixin(config=None)

    assert aws_config.config == {}


def test_aws_config_mixin_get_aws_profile_returns_profile_if_set():
    aws_config = aws.AWSConfigMixin(config=None)

    aws_config.config['aws_profile'] = 'expected profile'

    assert aws_config.get_aws_profile() == 'expected profile'


def test_aws_config_mixin_get_aws_profile_returns_none_if_not_set():
    aws_config = aws.AWSConfigMixin(config=None)

    assert aws_config.get_aws_profile() is None


def test_aws_config_mixin_remove_aws_profile_sets_to_none():
    aws_config = aws.AWSConfigMixin(config=None)

    aws_config.config['aws_profile'] = 'set'

    aws_config.remove_aws_profile()
    assert aws_config.config['aws_profile'] is None


def test_aws_config_mixin_set_to_use_credentials_sets_the_flag():
    aws_config = aws.AWSConfigMixin(config=None)

    aws_config.set_to_use_credentials()

    assert aws_config.config['use_credentials'] is True


def test_aws_config_mixin_get_region_name_returns_region_if_set():
    aws_config = aws.AWSConfigMixin(config=None)

    aws_config.config['region'] = 'region'

    assert aws_config.get_region_name() == 'region'


def test_aws_config_mixin_get_region_name_defaults_to_magnus_defaults(mocker, monkeypatch):
    mock_defaults = mocker.MagicMock()

    monkeypatch.setattr(aws, 'defaults', mock_defaults)
    mock_defaults.AWS_REGION = 'region'

    aws_config = aws.AWSConfigMixin(config=None)

    assert aws_config.get_region_name() == 'region'


def test_aws_config_mixin_get_aws_credentials_file_returns_from_config_if_set():
    aws_config = aws.AWSConfigMixin(config=None)

    aws_config.config['aws_credentials_file'] = 'aws credentials file'

    assert aws_config.get_aws_credentials_file() == 'aws credentials file'


def test_aws_config_mixin_get_aws_credentials_file_returns_default_aws_location_if_not_set():
    aws_config = aws.AWSConfigMixin(config=None)

    assert aws_config.get_aws_credentials_file() == str(aws.Path.home()) + '/.aws/credentials'


def test_aws_config_get_aws_credentials_from_env_raises_exception_if_keys_not_present_in_env():
    aws_config = aws.AWSConfigMixin(config=None)

    with pytest.raises(Exception):
        aws_config.get_aws_credentials_from_env()


def test_aws_config_get_aws_credentials_gets_env_vars(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'key')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'secret')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'token')

    aws_config = aws.AWSConfigMixin(config=None)

    aws_credentials = aws_config.get_aws_credentials_from_env()
    assert aws_credentials['AWS_ACCESS_KEY_ID'] == 'key'
    assert aws_credentials['AWS_SECRET_ACCESS_KEY'] == 'secret'
    assert aws_credentials['AWS_SESSION_TOKEN'] == 'token'
