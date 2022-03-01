import logging
from pathlib import Path
import fnmatch

from magnus.catalog import BaseCatalog
from magnus import utils
from magnus import defaults
from magnus.catalog import get_run_log_store, is_catalog_out_of_sync

logger = logging.getLogger(defaults.NAME)

try:
    from magnus_extension_aws_config.aws import AWSConfigMixin
except ImportError as _e:  # pragma: no cover
    msg = (
        'Please install magnus_extension_aws_config which provides the general utilities for AWS services.'
    )
    raise Exception(msg) from _e


class S3Catalog(BaseCatalog, AWSConfigMixin):
    """
    A S3 based catalog.

    Example config:
    catalog:
      type: s3
      config:
        compute_data_folder : data/
        s3_bucket: Bucket name
        region: Region if we are using
        aws_profile: The profile to use or default
    """
    service_name = 's3'

    def __init__(self, config, **kwargs):
        super().__init__(config, **kwargs)
        if not (self.config and 's3_bucket' in self.config):
            raise Exception('config with at least s3 bucket name should be provided for catalog type S3')

    def get(self, name, run_id, compute_data_folder=None, **kwargs):
        """Gets files from S3 store and moves to compute data folder

        Args:
            name (str): The glob pattern
            run_id (str): The ID of the current run
            compute_data_folder (str, optional): The path of the compute data to sync to. Defaults to None.

        Returns:
            list: List of data catalogs
        """
        copy_to = self.compute_data_folder
        if compute_data_folder:
            copy_to = compute_data_folder
        utils.safe_make_dir(copy_to)
        copy_to_path = Path(copy_to)

        s3_client = self.get_s3()
        self.check_s3_access(s3_client, self.get_bucket_name())

        # The run_id should be post fixed with a slash to avoid the JSON file being cataloged
        page_iter = s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=self.get_bucket_name(), Prefix=run_id + '/')

        for page in page_iter:
            print(list(file['Key'] for file in page['Contents']))

        search_name = f'{run_id}/{name}'
        if name == '*':
            search_name = '*'

        # TODO windows filename and fnmatch may not play nicely!
        s3_files = []
        for page in page_iter:
            s3_files += fnmatch.filter([file['Key'] for file in page['Contents']], search_name)

        data_catalogs = []
        run_log_store = get_run_log_store()
        for file in s3_files:
            file_obj = s3_client.get_object(Bucket=self.get_bucket_name(), Key=file)
            write_path = copy_to_path.joinpath(file.replace(f'{run_id}/', ''))
            with write_path.open('wb') as f:
                f.write(file_obj['Body'].read())

            data_catalog = run_log_store.create_data_catalog(file)
            data_catalog.catalog_handler_location = self.get_bucket_name()
            data_catalog.catalog_relative_path = str(file)
            data_catalog.data_hash = utils.get_data_hash(write_path)
            data_catalog.stage = 'get'
            data_catalogs.append(data_catalog)

        return data_catalogs

    def put(self, name, run_id, compute_data_folder=None, synced_catalogs=None, **kwargs):
        """Moves the data from the compute data folder to the s3 data catalog

        Args:
            name (str): The glob string pattern
            run_id (str): [description]
            compute_data_folder ([type], optional): [description]. Defaults to None.
            synced_catalogs ([type], optional): [description]. Defaults to None.

        Raises:
            Exception: [description]

        Returns:
            [type]: [description]
        """

        copy_from = self.compute_data_folder
        if compute_data_folder:
            copy_from = compute_data_folder

        if not utils.does_dir_exist(copy_from):
            raise Exception(f'Expected compute data folder to be present at: {copy_from} but not found')

        s3_client = self.get_s3()
        self.check_s3_access(s3_client, bucket=self.get_bucket_name())

        glob_files = Path(copy_from).glob(name)
        data_catalogs = []
        run_log_store = get_run_log_store()
        for file in glob_files:
            data_catalog = run_log_store.create_data_catalog(str(file.name))
            data_catalog.catalog_handler_location = self.get_bucket_name()
            data_catalog.catalog_relative_path = run_id + '/' + str(file.name)
            data_catalog.data_hash = utils.get_data_hash(str(file))
            data_catalog.stage = 'put'
            data_catalogs.append(data_catalog)

            if is_catalog_out_of_sync(data_catalog, synced_catalogs):
                logger.info(f'{data_catalog.name} was found to be changed, syncing')
                file_key = f'{run_id}/{file.name}'
                s3_client.upload_file(Filename=str(file.resolve()), Bucket=self.get_bucket_name(),
                                      Key=file_key)
            else:
                logger.info(f'{data_catalog.name} was found to be unchanged, ignoring syncing')
        return data_catalogs

    def get_s3(self):
        """Gets a s3 client object from a boto3 session.

        Returns:
            botocore.client: s3 client object
        """
        boto_session = self.get_boto3_session()
        return boto_session.client('s3')

    def get_bucket_name(self):
        """Gets the bucket name.

        Returns:
            str: Name of the S3 bucket for the catalog.
        """
        return self.config.get('s3_bucket')

    def check_s3_access(self, s3_client, bucket):
        """Checks access to a given S3 bucket

        Args:
            s3_client (botocore.client): S3 client
            bucket (str): Bucket name

        Raises:
            Exception: If can't access the bucket then an Exception is raised
        """
        check_access = s3_client.head_bucket(Bucket=bucket)

        if check_access['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(f'Expected Catalog s3 Bucket {bucket} does not exist, or you do not have access')

    def sync_between_runs(self, previous_run_id: str, run_id: str):
        """
        Given data catalogs from a previous run, sync them to the catalog of the run given by run_id

        Args:
            previous_run_id (str): The run_id of the previous run
            run_id (str): The run_id to which the data catalogs should be synced to.

        """
        s3_client = self.get_s3()
        self.check_s3_access(s3_client, self.get_bucket_name())

        s3_files = []
        page_iter = s3_client.get_paginator('list_objects_v2').paginate(Bucket=self.get_bucket_name(),
                                                                        Prefix=previous_run_id)
        for page in page_iter:
            s3_files += [file['Key'] for file in page['Contents']]

        for file in s3_files:
            s3_client.copy_object(Bucket=self.get_bucket_name(),
                                  CopySource=file,
                                  Key=file.replace(previous_run_id, run_id))
