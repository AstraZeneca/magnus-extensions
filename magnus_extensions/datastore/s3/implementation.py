import json
import logging
import tempfile

import botocore
from magnus import defaults, exceptions
from magnus.datastore import BaseRunLogStore, RunLog

logger = logging.getLogger(defaults.NAME)


try:
    from magnus_extensions.aws import AWSConfigMixin
except ImportError as _e:  # pragma: no cover
    msg = (
        "AWS Dependencies are not installed!!"
    )
    raise Exception(msg) from _e


class S3Store(BaseRunLogStore, AWSConfigMixin):
    """
    S3 implementation of Run Log store

    Example config:
    run_log_store:
      type: s3
      config:
        s3_bucket: The S3 bucket to use
        prefix: Any bucket prefix that you want to attach

    """
    service_name = 's3'

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
        return self.config.prefix.strip('/')

    def write_to_bucket(self, run_log: RunLog):
        """Writes the run log to S3 bucket

        Args:
            run_log (RunLog): The run log to write to bucket

        Raises:
            Exception: When the upload fails
        """
        boto3_session = self.get_boto3_session()
        s3_client = boto3_session.client('s3')
        temp_file = tempfile.NamedTemporaryFile()

        try:
            with open(temp_file.name, 'w', encoding='utf-8') as fw:
                json.dump(run_log.dict(), fw, ensure_ascii=True, indent=4)  # pylint: disable=no-member

            if self.prefix:
                object_key = f'{self.prefix}/{run_log.run_id}.json'
            else:
                object_key = f'{run_log.run_id}.json'

            s3_client.upload_file(temp_file.name, self.s3_bucket, object_key)
        except botocore.exceptions.ClientError as _e:
            if _e.response['Error']['Code'] == "404":
                logger.exception(f'Upload failed to {self.s3_bucket}')
            raise Exception(_e) from _e
        finally:
            temp_file.close()

    def get_from_bucket(self, run_id: str) -> RunLog:
        """
        Get a Run log object from the S3 bucket for the run id.

        Args:
            run_id (str): The run id for which we want the run log

        Raises:
            exceptions.RunLogNotFoundError: If the run log by the run id is not found
            Exception: If the access to bucket is not allowed

        Returns:
            RunLog: The run log of the run id
        """
        boto3_session = self.get_boto3_session()
        s3_client = boto3_session.client('s3')
        temp_file = tempfile.NamedTemporaryFile()

        try:
            if self.prefix:
                key = f'{self.prefix}/{run_id}.json'
            else:
                key = f'{run_id}.json'

            logger.info(f'Trying to download {key} from {self.s3_bucket}')
            s3_client.download_file(self.s3_bucket, key, temp_file.name)

            json_str = json.load(open(temp_file.name, 'rb'))
            run_log = RunLog(**json_str)
            return run_log
        except botocore.exceptions.ClientError as _e:
            if _e.response['Error']['Code'] == "404":
                logger.exception(f'{key} does not exist in {self.s3_bucket}')
                raise exceptions.RunLogNotFoundError(run_id)
            raise
        finally:
            temp_file.close()

    def create_run_log(self, run_id: str, dag_hash: str = '', use_cached: bool = False,
                       tag: str = '', original_run_id: str = '', status: str = defaults.CREATED, **kwargs) -> RunLog:
        # Creates a Run log
        # Adds it to the db
        try:
            self.get_run_log_by_id(run_id=run_id, full=False)
            raise exceptions.RunLogExistsError(run_id=run_id)
        except exceptions.RunLogNotFoundError:
            pass

        logger.info(f'{self.service_name} Creating a Run Log for : {run_id}')
        run_log = RunLog(run_id=run_id, dag_hash=dag_hash, use_cached=use_cached,
                         tag=tag, original_run_id=original_run_id, status=status)
        self.write_to_bucket(run_log)
        return run_log

    def get_run_log_by_id(self, run_id, full=True, **kwargs):
        # Returns the run_log defined by id
        # Raises Exception if not found
        try:
            logger.info(f'{self.service_name} Getting a Run Log for : {run_id}')
            run_log = self.get_from_bucket(run_id)
            return run_log
        except Exception as e:
            raise exceptions.RunLogNotFoundError(run_id) from e

    def put_run_log(self, run_log, **kwargs):
        # Puts the run_log into the database
        logger.info(f'{self.service_name} Putting the run log in the DB: {run_log.run_id}')
        self.write_to_bucket(run_log)
