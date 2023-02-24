import fnmatch
import json
import logging
import tempfile
import time
from pathlib import Path
from string import Template
from typing import Any, List, Optional, Union

import botocore
from magnus import defaults
from magnus.datastore import ChunkedFileSystemRunLogStore

logger = logging.getLogger(defaults.NAME)


try:
    from magnus_extensions.aws import AWSConfigMixin
except ImportError as _e:  # pragma: no cover
    msg = (
        "AWS Dependencies are not installed!!"
    )
    raise Exception(msg) from _e


class ChunkedS3Store(ChunkedFileSystemRunLogStore, AWSConfigMixin):
    """
    S3 implementation of Chunked Run Log store

    Example config:
    run_log_store:
      type: s3
      config:
        s3_bucket: The S3 bucket to use
        prefix: Any bucket prefix that you want to attach

    """
    service_name = 'chunked-s3'
    LogTypes = ChunkedFileSystemRunLogStore.LogTypes

    class Config(AWSConfigMixin.Config):
        s3_bucket: str
        prefix: str = ''

    @property
    def s3_bucket(self) -> str:
        """
        Returns the S3 bucket name from the config

        Returns:
            str: The name of the s3 bucket as per the config
        """
        return self.config.s3_bucket

    @property
    def prefix(self):
        """
        Return the prefix if present in the config. Or an empty string
        """
        return Path(self.config.prefix.strip('/'))

    def prefix_with_run_id(self, run_id: str) -> Path:
        return self.prefix / run_id

    def get_matches(self, run_id: str, name: str, multiple_allowed: bool = False) -> Optional[Union[List[Path], Path]]:
        """
        Get contents of files matching the pattern name*

        Args:
            run_id (str): The run id
            name (str): The suffix of the file name to check in the run log store.
        """
        sub_name = Template(name).safe_substitute({"creation_time": ""})
        search_name = str(f'{self.prefix_with_run_id(run_id=run_id)}/{sub_name}*')

        boto3_session = self.get_boto3_session()
        s3_client = boto3_session.client('s3')
        page_iter = s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=self.s3_bucket, Prefix=str(self.prefix_with_run_id(run_id=run_id)))

        matches = []
        for page in page_iter:
            try:
                matches += fnmatch.filter([file['Key'] for file in page['Contents']], search_name)
            except KeyError:
                logger.warning(f"Did not find any objects matching the pattern")
                return None

        if not matches:
            return None

        if not multiple_allowed:
            if len(matches) > 1:
                msg = (
                    f"Multiple matches found for {name} while multiple is not allowed"
                )
                raise Exception(msg)
            return Path(matches[0])

        return [Path(match) for match in matches]

    def _store(self, run_id: str, contents: dict, name: Path):
        """
        Store the contents against the name in the folder.

        Args:
            run_id (str): The run id
            contents (dict): The dict to store
            name (str): The name to store as
        """
        boto3_session = self.get_boto3_session()
        s3_client = boto3_session.client('s3')

        with tempfile.NamedTemporaryFile() as temp_file:
            with open(temp_file.name, 'w', encoding='utf-8') as fw:
                json.dump(contents, fw, ensure_ascii=True, indent=4)  # pylint: disable=no-member

            object_key = self.prefix_with_run_id(run_id=run_id) / name.name
            try:
                s3_client.upload_file(temp_file.name, self.s3_bucket, str(object_key))
            except botocore.exceptions.ClientError as _e:
                if _e.response['Error']['Code'] == "404":
                    logger.exception(f'Upload failed to {self.s3_bucket}')
                raise Exception(_e) from _e

    def _retrieve(self, name: Path) -> dict:
        """
        Does the job of retrieving from the folder.

        Args:
            name (str): the name of the file to retrieve

        Returns:
            dict: The contents
        """
        boto3_session = self.get_boto3_session()
        s3_client = boto3_session.client('s3')

        with tempfile.NamedTemporaryFile() as temp_file:
            key = str(name)

            logger.info(f'Trying to download {key} from {self.s3_bucket}')
            try:
                s3_client.download_file(self.s3_bucket, key, temp_file.name)
                json_str = json.load(open(temp_file.name, 'rb'))
                return json_str
            except botocore.exceptions.ClientError as _e:
                if _e.response['Error']['Code'] == "404":
                    logger.exception(f'{key} does not exist in {self.s3_bucket}')
                    raise _e

    def store(self, run_id: str, log_type: LogTypes, contents: dict, name: str = ''):
        """Store a SINGLE log type in the file system

        Args:
            run_id (str): The run id to store against
            log_type (LogTypes): The type of log to store
            contents (dict): The dict of contents to store
            name (str, optional): The name against the contents have to be stored. Defaults to ''.
        """

        naming_pattern = self.naming_pattern(log_type=log_type, name=name)
        match = self.get_matches(run_id=run_id, name=naming_pattern, multiple_allowed=False)
        # The boolean multiple allowed confuses mypy a lot!
        name_to_give: Path = None  # type: ignore
        if match:
            existing_contents = self._retrieve(name=match)  # type: ignore
            contents = dict(existing_contents, **contents)
            name_to_give = match  # type: ignore
        else:
            _name = Template(naming_pattern).safe_substitute({"creation_time": str(int(time.time_ns()))})
            name_to_give = self.prefix_with_run_id(run_id=run_id) / _name

        self._store(run_id=run_id, contents=contents, name=name_to_give)

    def retrieve(self, run_id: str, log_type: LogTypes, name: str = '', multiple_allowed=False) -> Any:
        """
        Retrieve the model given a log_type and a name.
        Use multiple_allowed to control if you are expecting multiple of them.
        eg: There could be multiple of Parameters- but only one of StepLog-stepname

        The reasoning for name to be defaulted to empty string:
            Its actually conditionally empty. For RunLog and Parameter it is empty.
            For StepLog and BranchLog it should be provided.

        Args:
            run_id (str): The run id
            log_type (LogTypes): One of RunLog, Parameter, StepLog, BranchLog
            name (str, optional): The name to match. Defaults to ''.
            multiple_allowed (bool, optional): Are multiple allowed. Defaults to False.

        Raises:
            FileNotFoundError: If there is no match found

        Returns:
            Any: One of StepLog, BranchLog, Parameter or RunLog
        """
        if not name and log_type not in [self.LogTypes.RUN_LOG, self.LogTypes.PARAMETER]:
            raise Exception(f"Name is required during retrieval for {log_type}")

        naming_pattern = self.naming_pattern(log_type=log_type, name=name)
        matches = self.get_matches(run_id=run_id, name=naming_pattern, multiple_allowed=multiple_allowed)

        if matches:
            if not multiple_allowed:
                contents = self._retrieve(name=matches)  # type: ignore
                model = self.ModelTypes[log_type.name].value
                return model(**contents)

            models = []
            for match in matches:  # type: ignore
                contents = self._retrieve(name=match)
                model = self.ModelTypes[log_type.name].value
                models.append(model(**contents))
            return models

        raise FileNotFoundError()
