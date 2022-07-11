import logging
import json
from typing import OrderedDict, Union


from magnus.datastore import BaseRunLogStore, RunLog, StepLog, BranchLog
from magnus.datastore import DataCatalog, CodeIdentity, StepAttempt
from magnus import defaults
from magnus import exceptions

logger = logging.getLogger(defaults.NAME)
logger.info('Loading partitioned file system datastore extension')


class PartitionedDataStore(BaseRunLogStore):
    """
    The base class of a Run Log Store with many common methods implemented.

    Attribute types are useful when accessing different properties of the run log store.
    For example, if you want to access parameter 'x', you need to query the persistence layer with
    PARAMETER ATTRIBUTE TYPE and ATTRIBUTE KEY of x.

    Note: As a general guideline, do not extract anything from the config to set class level attributes.
          Integration patterns modify the config after init to change behaviors.
          Access config properties using getters/property of the class.
    """
    service_name = ''

    def __init__(self, config):
        super().__init__(config)

        # Attribute types
        self.RUNLOG_ATTRIBUTE_TYPE = 'RunLog'  # pylint: disable=c0103
        self.PARAMETER_ATTRIBUTE_TYPE = 'Parameter'  # pylint: disable=c0103
        self.STEPLOG_ATTRIBUTE_TYPE = 'StepLog'  # pylint: disable=c0103
        self.BRANCHLOG_ATTRIBUTE_TYPE = 'BranchLog'  # pylint: disable=c0103

    @property
    def attribute_name_template(self):
        """
        The naming template to be used to access a attribute of the run log.

        Individual classes should implement this.
        Raises:
            NotImplementedError: This is a base class and therefore has no default implementation
        """
        raise NotImplementedError

    def _persist(self, run_id: str, attribute_key: str, attribute_type: str, attribute_value: Union[dict, str]):
        """
        Persist the attribute of the run log against the specified run id.

        Args:
            run_id (str): The run id to update the run log
            attribute_key (str): run_log for RunLog, the step log internal name for steps, parameter key for parameter
            attribute_type (str): One of RunLog, Parameter, StepLog, BranchLog
            attribute_value (str): The value to store
        Raises:
            NotImplementedError: This is a base class and therefore has no default implementation
        """
        raise NotImplementedError

    def _retrieve(self, run_id: str, attribute_type: str, attribute_key=None) -> dict:
        """
        Retrieve an attribute of the run log.
        The attribute could be a RunLog, StepLog, BranchLog or Parameter.

        The returned records should be ordered chronologically.

        Args:
            run_id (str): The Run Id to retrieve the attribute
            attribute_type (str): The attribute to retrieve from the DB for the run_id
            attribute_key (str, optional): Conditional filtering of attribute. Defaults to None.

        Returns:
            dict: All the records from the run log which match the run_id and attribute
        Raises:
            NotImplementedError: This is a base class and therefore has no default implementation
        """
        raise NotImplementedError

    def _get_parent_branch(self, name: str) -> Union[str, None]:  # pylint: disable=R0201
        """
        Returns the name of the parent branch.
        If the step is part of main dag, return None.

        Args:
            name (str): The name of the step.

        Returns:
            str: The name of the branch containing the step.
        """
        dot_path = name.split('.')

        if len(dot_path) == 1:
            return None
        # Ignore the step name
        return '.'.join(dot_path[:-1])

    def _get_parent_step(self, name: str) -> Union[str, None]:  # pylint: disable=R0201
        """
        Returns the step containing the step, useful when we have steps within a branch.
        Returns None, if the step belongs to parent dag.

        Args:
            name (str): The name of the step to find the parent step it belongs to.

        Returns:
            str: The parent step the step belongs to, None if the step belongs to parent dag.
        """
        dot_path = name.split('.')

        if len(dot_path) == 1:
            return None
        # Ignore the branch.step_name
        return '.'.join(dot_path[:-2])

    def _prepare_full_run_log(self, run_log: RunLog) -> RunLog:
        """
        Populates the run log with the branches and steps.

        Args:
            run_log (RunLog): The partial run log containing empty step logs
        """
        run_id = run_log.run_id
        run_log.parameters = self.get_parameters(run_id=run_id)

        all_steps = self._retrieve(run_id=run_id, attribute_type=self.STEPLOG_ATTRIBUTE_TYPE)
        all_branches = self._retrieve(run_id=run_id, attribute_type=self.BRANCHLOG_ATTRIBUTE_TYPE)

        ordered_steps = OrderedDict()
        for name, step in all_steps.items():
            json_str = step
            ordered_steps[name] = StepLog(**json_str)

        ordered_branches = OrderedDict()
        for name, branch in all_branches.items():
            json_str = branch
            ordered_branches[name] = BranchLog(**json_str)

        current_branch = None
        for step_internal_name in ordered_steps:
            current_branch = self._get_parent_branch(step_internal_name)
            step_to_add_branch = self._get_parent_step(step_internal_name)

            if not current_branch:
                current_branch = run_log
            else:
                current_branch = ordered_branches[current_branch]
                step_to_add_branch = ordered_steps[step_to_add_branch]
                step_to_add_branch.branches[current_branch.internal_name] = current_branch

            current_branch.steps[step_internal_name] = ordered_steps[step_internal_name]

    def create_run_log(self, run_id: str, **kwargs):
        """
        Creates a Run Log object by using the config

        Logically the method should do the following:
            * Creates a Run log
            * Adds it to the db
            * Return the log
        """

        logger.info(f'{self.service_name} Creating a Run Log for : {run_id}')
        run_log = RunLog(run_id=run_id, status=defaults.CREATED)
        self._persist(run_id=run_id,
                      attribute_key='run_log',
                      attribute_type=self.RUNLOG_ATTRIBUTE_TYPE,
                      attribute_value=run_log.dict())  # pylint: disable=no-member

        return run_log

    def get_run_log_by_id(self, run_id: str, full: bool = True, **kwargs) -> RunLog:
        """
        Retrieves a Run log from the database using the config and the run_id

        Args:
            run_id (str): The run_id of the run
            full (bool): return the full run log store or only the RunLog object

        Returns:
            RunLog: The RunLog object identified by the run_id

        Logically the method should:
            * Returns the run_log defined by id from the data store defined by the config

        Raises:
            RunLogNotFoundError: If the run log for run_id is not found in the datastore
        """

        try:
            logger.info(f'{self.service_name} Getting a Run Log for : {run_id}')
            records = self._retrieve(run_id=run_id,
                                     attribute_key='run_log',
                                     attribute_type=self.RUNLOG_ATTRIBUTE_TYPE)
            if not records:
                raise exceptions.RunLogNotFoundError(run_id)

            json_str = list(records.values())[0]

            run_log = RunLog(**json_str)

            if full:
                self._prepare_full_run_log(run_log)
            return run_log
        except:
            raise exceptions.RunLogNotFoundError(run_id)

    def put_run_log(self, run_log: RunLog, **kwargs):
        """
        Puts the Run Log in the database as defined by the config

        Args:
            run_log (RunLog): The Run log of the run

        Logically the method should:
            Puts the run_log into the database

        """
        run_id = run_log.run_id
        logger.info(f'{self.service_name} Putting the Run Log for : {run_id}')

        self._persist(run_id=run_id,
                      attribute_key='run_log',
                      attribute_type=self.RUNLOG_ATTRIBUTE_TYPE,
                      attribute_value=run_log.dict())  # pylint: disable=no-member

    def get_parameters(self, run_id: str, **kwargs) -> dict:  # pylint: disable=unused-argument
        """
        Get the parameters from the Run log defined by the run_id

        Args:
            run_id (str): The run_id of the run

        The method should:
            * Call get_run_log_by_id(run_id) to retrieve the run_log
            * Return the parameters as identified in the run_log

        Returns:
            dict: A dictionary of the run_log parameters
        Raises:
            RunLogNotFoundError: If the run log for run_id is not found in the datastore
        """
        logger.info(f'{self.service_name} Getting parameters for : {run_id}')
        records = self._retrieve(run_id=run_id, attribute_type=self.PARAMETER_ATTRIBUTE_TYPE)

        return records

    def set_parameters(self, run_id: str, parameters: dict, **kwargs):  # pylint: disable=unused-argument
        """
        Update the parameters of the Run log with the new parameters

        This method would over-write the parameters, if the parameter exists in the run log already

        The method should:
            * Call get_run_log_by_id(run_id) to retrieve the run_log
            * Update the parameters of the run_log
            * Call put_run_log(run_log) to put the run_log in the datastore

        Args:
            run_id (str): The run_id of the run
            parameters (dict): The parameters to update in the run log
        Raises:
            RunLogNotFoundError: If the run log for run_id is not found in the datastore
        """

        logger.info(f'{self.service_name} setting parameters for : {run_id}')
        for key, value in parameters.items():
            self._persist(run_id=run_id,
                          attribute_type=self.PARAMETER_ATTRIBUTE_TYPE,
                          attribute_key=key,
                          attribute_value=value
                          )

    def get_run_config(self, run_id: str, **kwargs) -> dict:  # pylint: disable=unused-argument
        """
        Given a run_id, return the run_config used to perform the run.

        Args:
            run_id (str): The run_id of the run

        Returns:
            dict: The run config used for the run
        """

        run_log = self.get_run_log_by_id(run_id=run_id)
        return run_log.run_config

    def set_run_config(self, run_id: str, run_config: dict, **kwargs):  # pylint: disable=unused-argument
        """ Set the run config used to run the run_id

        Args:
            run_id (str): The run_id of the run
            run_config (dict): The run_config of the run
        """

        run_log = self.get_run_log_by_id(run_id=run_id)
        run_log.run_config.update(run_config)
        self.put_run_log(run_log=run_log)

    def create_step_log(self, name: str, internal_name: str, **kwargs):  # pylint: disable=unused-argument
        """
        Create a step log by the name and internal name

        The method does not update the Run Log with the step log at this point in time.
        This method is just an interface for external modules to create a step log

        Args:
            name (str): The friendly name of the step log
            internal_name (str): The internal naming of the step log. The internal naming is a dot path convention

        Returns:
            StepLog: A uncommitted step log object
        """
        logger.info(f'{self.service_name} Creating a Step Log: {name}')
        return StepLog(name=name, internal_name=internal_name, status=defaults.CREATED)

    def get_step_log(self, internal_name: str, run_id: str, **kwargs) -> StepLog:  # pylint: disable=unused-argument
        """
        Get a step log from the datastore for run_id and the internal naming of the step log

        The internal naming of the step log is a dot path convention.

        The method should:
            * Call get_run_log_by_id(run_id) to retrieve the run_log
            * Identify the step location by decoding the internal naming
            * Return the step log

        Args:
            internal_name (str): The internal name of the step log
            run_id (str): The run_id of the run

        Returns:
            StepLog: The step log object for the step defined by the internal naming and run_id

        Raises:
            RunLogNotFoundError: If the run log for run_id is not found in the datastore
            StepLogNotFoundError: If the step log for internal_name is not found in the datastore for run_id
        """
        logger.info(f'{self.service_name} Getting the step log: {internal_name} of {run_id}')

        records = self._retrieve(run_id=run_id, attribute_type=self.STEPLOG_ATTRIBUTE_TYPE,
                                 attribute_key=internal_name)
        if len(records) > 1:
            raise Exception(f'{self.service_name} Getting too many step logs for {internal_name}')

        if not records:
            raise exceptions.StepLogNotFoundError(run_id=run_id, name=internal_name)

        json_str = list(records.values())[0]
        return StepLog(**json_str)

    def add_step_log(self, step_log: StepLog, run_id: str, **kwargs):  # pylint: disable=unused-argument
        """
        Add the step log in the run log as identified by the run_id in the datastore

        The method should:
             * Call get_run_log_by_id(run_id) to retrieve the run_log
             * Identify the branch to add the step by decoding the step_logs internal name
             * Add the step log to the identified branch log
             * Call put_run_log(run_log) to put the run_log in the datastore

        Args:
            step_log (StepLog): The Step log to add to the database
            run_id (str): The run id of the run

        Raises:
            RunLogNotFoundError: If the run log for run_id is not found in the datastore
            BranchLogNotFoundError: If the branch of the step log for internal_name is not found in the datastore
                                    for run_id
        """
        logger.info(f'{self.service_name} Adding the step log to DB: {step_log.name}')
        self._persist(run_id=run_id,
                      attribute_key=step_log.internal_name,
                      attribute_type=self.STEPLOG_ATTRIBUTE_TYPE,
                      attribute_value=step_log.dict())  # pylint: disable=no-member

    def create_branch_log(self, internal_branch_name: str, **kwargs) -> BranchLog:  # pylint: disable=unused-argument
        """
        Creates a uncomitted branch log object by the internal name given

        Args:
            internal_branch_name (str): Creates a branch log by name internal_branch_name

        Returns:
            BranchLog: Uncommitted and initialized with defaults BranchLog object
        """
        # Create a new BranchLog
        logger.info(f'{self.service_name} Creating a Branch Log : {internal_branch_name}')
        return BranchLog(internal_name=internal_branch_name, status=defaults.CREATED)

    def get_branch_log(self, internal_branch_name: str, run_id: str, **kwargs) -> Union[BranchLog, RunLog]:  # pylint: disable=unused-argument
        """
        Returns the branch log by the internal branch name for the run id

        If the internal branch name is none, returns the run log

        Args:
            internal_branch_name (str): The internal branch name to retrieve.
            run_id (str): The run id of interest

        Returns:
            BranchLog: The branch log or the run log as requested.
        """
        logger.info(f'{self.service_name} Getting the Branch log: {internal_branch_name} of {run_id}')

        if not internal_branch_name:
            return self.get_run_log_by_id(run_id=run_id)

        records = self._retrieve(run_id=run_id, attribute_type=self.BRANCHLOG_ATTRIBUTE_TYPE,
                                 attribute_key=internal_branch_name)
        if len(records) > 1:
            raise Exception(f'{self.service_name} Getting too many branch logs for {internal_branch_name}')

        if not records:
            raise exceptions.BranchLogNotFoundError(run_id=run_id, name=internal_branch_name)

        json_str = list(records.values())[0]
        return BranchLog(**json_str)

    def add_branch_log(self, branch_log: Union[BranchLog, RunLog], run_id: str, **kwargs):  # pylint: disable=unused-argument
        """
        The method should:
        # Get the run log
        # Get the branch and step containing the branch
        # Add the branch to the step
        # Write the run_log

        The branch log could some times be a Run log and should be handled appropriately

        Args:
            branch_log (BranchLog): The branch log/run log to add to the database
            run_id (str): The run id to which the branch/run log is added
        """

        if type(branch_log) == RunLog:
            self.put_run_log(run_log=branch_log)
            return
        logger.info(f'{self.service_name} Adding the Branch log to DB: {branch_log.internal_name}')
        self._persist(run_id=run_id,
                      attribute_key=branch_log.internal_name,
                      attribute_type=self.BRANCHLOG_ATTRIBUTE_TYPE,
                      attribute_value=branch_log.dict())

    def create_attempt_log(self, **kwargs) -> StepAttempt:  # pylint: disable=unused-argument
        """
        Returns an uncommitted step attempt log.

        Returns:
            StepAttempt: An uncommitted step attempt log
        """
        logger.info(f'{self.service_name} Creating an attempt log')
        return StepAttempt()

    def create_code_identity(self, **kwargs) -> CodeIdentity:  # pylint: disable=unused-argument
        """
        Creates an uncommitted Code identity class

        Returns:
            CodeIdentity: An uncommitted code identity class
        """
        logger.info(f'{self.service_name} Creating Code identity')
        return CodeIdentity()

    def create_data_catalog(self, name: str, **kwargs) -> DataCatalog:  # pylint: disable=unused-argument
        """
        Create a uncommitted data catalog object

        Args:
            name (str): The name of the data catalog item to put

        Returns:
            DataCatalog: The DataCatalog object.
        """
        logger.info(f'{self.service_name} Creating Data Catalog for {name}')
        return DataCatalog(name=name)
