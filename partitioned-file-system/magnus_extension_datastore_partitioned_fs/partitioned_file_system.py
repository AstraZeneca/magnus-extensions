import logging
import json
from string import Template as str_template
from pathlib import Path
from typing import Union


from magnus import defaults, utils


from magnus_extension_datastore_partitioned_fs.base import PartitionDataStore

logger = logging.getLogger(defaults.NAME)
logger.info('Loading partitioned file system datastore extension')


class PartitionedFileSystem(PartitionDataStore):
    """
    Using partitioned file system as a run log store.
    All concurrently accessible attributes are stored in independent files.

    Example config:
    run_log:
      type: partitioned-file-system
      config:
        log_folder: The folder to out the logs. Defaults to .run_log_store
    """
    service_name = 'partitioned-file-system'
    CONFIG_KEY_LOG_FOLDER = 'log_folder'

    @property
    def attribute_name_template(self):
        """
        The naming template to be used to access a attribute of the run log.

        Individual classes should implement this.
        Raises:
            NotImplementedError: This is a base class and therefore has no default implementation
        """
        return str_template('${run_id}%${attribute_type}%${attribute_key}.json')

    @property
    def log_folder_name(self) -> str:
        """
        The log folder to save logs

        Returns:
            str: The path of the log folder
        """
        if self.config:
            return self.config.get(self.CONFIG_KEY_LOG_FOLDER, defaults.LOG_LOCATION_FOLDER)

        return defaults.LOG_LOCATION_FOLDER

    def _persist(self, run_id: str, attribute_key: str, attribute_type: str, attribute_value: Union[dict, str]):
        """
        Write a part of the Run log to the file system against a specified run_id

        Args:
            run_id (str): The run id to update the run log
            attribute_key (str): run_log for RunLog, the step log internal name for steps, parameter key for parameter
            attribute_type (str): One of RunLog, Parameter, StepLog, BranchLog
            attribute_value (str): The value to store
        """
        file_name = self.attribute_name_template.safe_substitute(
            {'run_id': run_id, 'attribute_type': attribute_type, 'attribute_key': attribute_key})

        write_to = self.log_folder_name
        utils.safe_make_dir(write_to)

        write_to_path = Path(write_to)

        json_file_path = write_to_path / file_name

        with json_file_path.open('w') as fw:
            json.dump(attribute_value, fw, ensure_ascii=True, indent=4)  # pylint: disable=no-member

    def _retrieve(self, run_id: str, attribute_type: str, attribute_key=None):
        """
        Gets an attribute from the file system.
        The attribute could be a RunLog, StepLog, BranchLog or Parameter

        Args:
            run_id (str): The Run Id to retrieve the attribute
            attribute_type (str): The attribute to retrieve from the DB for the run_id
            attribute_key (str, optional): Conditional filtering of attribute. Defaults to None.

        Returns:
            object: All the records from the DB which match the run_id and attribute
        """
        filter_condition = f'{run_id}%{attribute_type}*'

        if attribute_key:
            filter_condition = f'{run_id}%{attribute_type}%{attribute_key}.json'

        read_from_path = Path(self.log_folder_name)
        glob_files = read_from_path.glob(filter_condition)

        records = {}
        for file in glob_files:
            key = file.name[:-5].split('%')[-1]
            if attribute_key:
                key = attribute_key

            records[key] = json.load(open(file, encoding='utf-8'))

        return records
