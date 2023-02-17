import datetime
import json
import logging
from collections import OrderedDict
from string import Template as str_template

from magnus import defaults, exceptions
from magnus.datastore import BaseRunLogStore, BranchLog, RunLog, StepLog

logger = logging.getLogger(defaults.NAME)
logger.info('Loading DB datastore extension')


try:
    import sqlalchemy
    from sqlalchemy import Column, DateTime, Integer, Sequence, Text
    from sqlalchemy.orm import declarative_base, sessionmaker

    Base = declarative_base()

    class DBLog(Base):
        """
        Base table for storing run logs in database.

        In this model, we fragment the run log into logical units that are concurrent safe.
        """
        __tablename__ = 'db_log'
        pk = Column(Integer, Sequence('id_seq'), primary_key=True)
        run_id = Column(Text)
        attribute_key = Column(Text)  # run_log, step_internal_name, parameter_key etc
        attribute_type = Column(Text)  # RunLog, Step, Branch, Parameter
        attribute_value = Column(Text)  # The JSON string
        created_at = Column(DateTime, default=datetime.datetime.utcnow)

        def __repr__(self):
            return f'DBLog for {self.run_id} and {self.attribute_key}'

except ImportError as _e:
    logger.exception('Unable to import SQLalchemy, is it installed?')
    msg = (
        "SQLAlchemy is required for this extension. Please install it"
    )
    raise Exception(msg) from _e


def create_tables(connection_string):
    """
    from magnus.datastore_extensions import db
    connection_string = 'postgresql://localhost/'
    db.create_tables(connection_string)
    """
    engine = sqlalchemy.create_engine(connection_string)
    Base.metadata.create_all(engine)


class DBStore(BaseRunLogStore):
    """
    Using SQL alchemy to interface for a database as a Run Log store.
    All concurrently accessible attributes are stored in independent rows

    The expected columns:
        run_id : str,
        attribute_key: str,  # step name, branch name, parameter name etc
        attribute_type: str, # step, branch, parameter, run_log
        attribute_value: json, # string representation of JSON
        created_at: UTC time now,  #Needed for ordering the Run log

    Example config:
    run_log:
      type: db
      config:
        connection_string: The connection string to use in SQLAlchemy. Secret placeholders are fine.
    """
    service_name = 'db'

    def __init__(self, config):
        super().__init__(config)
        if 'connection_string' not in self.config:
            raise Exception('Run Log stores of DB should have a connection string')

        self.engine = None  # Follows a singleton pattern
        self.session = None

        # Attribute Types
        self.RUNLOG_ATTRIBUTE_TYPE = 'RunLog'  # pylint: disable=c0103
        self.PARAMETER_ATTRIBUTE_TYPE = 'Parameter'  # pylint: disable=c0103
        self.STEPLOG_ATTRIBUTE_TYPE = 'StepLog'  # pylint: disable=c0103
        self.BRANCHLOG_ATTRIBUTE_TYPE = 'BranchLog'  # pylint: disable=c0103

    @property
    def connection_string(self) -> str:
        """
        Returns the connection string as provided by the config

        Raises:
            Exception: If connection_string is not provided in the config

        Returns:
            str: The connection string specified in the config
        """
        return self.config['connection_string']

    def _make_db_engine(self):
        """
        Creates a DB engine and session object once per execution. Singleton pattern
        """
        if not self.engine:
            from magnus.pipeline import \
                global_executor  # pylint: disable=C0415

            secrets = global_executor.secrets_handler.get()  # Returns all secrets as dictionary
            connection_string = str_template(self.connection_string).safe_substitute(**secrets)

            self.engine = sqlalchemy.create_engine(connection_string, pool_pre_ping=True)
            self.session = sessionmaker(bind=self.engine)

    def write_to_db(self, run_id: str, attribute_key: str, attribute_type: str, attribute_value: str):
        """
        Write a part of the Run log to the database against a specified run_id

        Args:
            run_id (str): The run id to update the run log
            attribute_key (str): run_log for RunLog, the step log internal name for steps, parameter key for parameter
            attribute_type (str): One of RunLog, Parameter, StepLog, BranchLog
            attribute_value (str): The value to put in the database
        """
        self._make_db_engine()

        with self.session() as session:
            record = DBLog(run_id=run_id, attribute_key=attribute_key,
                           attribute_type=attribute_type, attribute_value=attribute_value)
            session.add(record)
            session.commit()

    def update_db(self, run_id: str, attribute_type: str, attribute_key: str, attribute_value: str, upsert=True):
        """
        Update the Database with a part of run log.

        If upsert is true, we create the object instead of updating.

        Args:
            run_id (str): The run id to update the run log
            attribute_key (str): run_log for RunLog, the step log internal name for steps, parameter key for parameter
            attribute_type (str): One of RunLog, Parameter, StepLog, BranchLog
            attribute_value (str): The value to put in the database
            upsert (bool, optional): Create if not present. Defaults to True.
        """
        self._make_db_engine()

        with self.session() as session:
            records = session.query(DBLog).filter(DBLog.run_id == run_id). \
                filter(DBLog.attribute_type == attribute_type).\
                filter(DBLog.attribute_key == attribute_key).all()

            for record in records:
                record.attribute_value = attribute_value

            if upsert and len(records) == 0:
                # We insert otherwise
                record = DBLog(run_id=run_id, attribute_key=attribute_key,
                               attribute_type=attribute_type, attribute_value=attribute_value)
                session.add(record)

            session.commit()

    def get_from_db(self, run_id: str, attribute_type: str, attribute_key=None):
        """
        Gets an attribute from the db.
        The attribute could be a RunLog, StepLog, BranchLog or Parameter

        Args:
            run_id (str): The Run Id to retrieve the attribute
            attribute_type (str): The attribute to retrieve from the DB for the run_id
            attribute_key (str, optional): Conditional filtering of attribute. Defaults to None.

        Returns:
            object: All the records from the DB which match the run_id and attribute
        """
        self._make_db_engine()

        with self.session() as session:
            query = session.query(DBLog).order_by(DBLog.created_at).filter(DBLog.run_id == run_id). \
                filter(DBLog.attribute_type == attribute_type)

            if attribute_key:
                query = query.filter(DBLog.attribute_key == attribute_key)
            records = query.all()

            return records

    def _get_parent_branch(self, name: str) -> str:  # pylint: disable=R0201
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

    def _get_parent_step(self, name: str) -> str:  # pylint: disable=R0201
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

    def prepare_full_run_log(self, run_log: RunLog) -> RunLog:
        """
        Populates the run log with the branches and steps.
        In database mode, we fragment the run log into individual Run Logs and this method populates the
        full run log by querying the underlying tables.

        Args:
            run_log (RunLog): The partial run log containing empty step logs
        """
        run_id = run_log.run_id
        run_log.parameters = self.get_parameters(run_id=run_id)

        all_steps = self.get_from_db(run_id=run_id, attribute_type=self.STEPLOG_ATTRIBUTE_TYPE)
        all_branches = self.get_from_db(run_id=run_id, attribute_type=self.BRANCHLOG_ATTRIBUTE_TYPE)

        ordered_steps = OrderedDict()
        for step in all_steps:
            json_str = json.loads(step.attribute_value)
            ordered_steps[step.attribute_key] = StepLog(**json_str)

        ordered_branches = OrderedDict()
        for branch in all_branches:
            json_str = json.loads(branch.attribute_value)
            ordered_branches[branch.attribute_key] = BranchLog(**json_str)

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

    def create_run_log(self, run_id, **kwargs):
        """
        Creates a Run Log object by using the config

        Logically the method should do the following:
            * Creates a Run log
            * Adds it to the db
            * Return the log
        """
        logger.info(f'{self.service_name} Creating a Run Log for : {run_id}')
        run_log = RunLog(run_id=run_id, status=defaults.CREATED)
        self.write_to_db(run_id=run_id,
                         attribute_key='run_log',
                         attribute_type=self.RUNLOG_ATTRIBUTE_TYPE,
                         attribute_value=json.dumps(run_log.dict(), ensure_ascii=True))  # pylint: disable=no-member

        return run_log

    def get_run_log_by_id(self, run_id, full=True, **kwargs):
        """
        Retrieves a Run log from the database using the config and the run_id

        Args:
            run_id (str): The run_id of the run

        Logically the method should:
            * Returns the run_log defined by id from the data store defined by the config

        Raises:
            RunLogNotFoundError: If the run log for run_id is not found in the datastore
        """
        try:
            logger.info(f'{self.service_name} Getting a Run Log for : {run_id}')
            records = self.get_from_db(run_id=run_id,
                                       attribute_key='run_log',
                                       attribute_type=self.RUNLOG_ATTRIBUTE_TYPE)
            if not records:
                raise exceptions.RunLogNotFoundError(run_id)

            json_str = json.loads(records[0].attribute_value)

            run_log = RunLog(**json_str)

            if full:
                self.prepare_full_run_log(run_log)
            return run_log
        except:
            raise exceptions.RunLogNotFoundError(run_id)

    def put_run_log(self, run_log, **kwargs):
        """
        Puts the Run Log in the database as defined by the config

        Args:
            run_log (RunLog): The Run log of the run

        Logically the method should:
            Puts the run_log into the database
        """
        run_id = run_log.run_id
        logger.info(f'{self.service_name} Putting the Run Log for : {run_id}')
        self.update_db(run_id=run_id,
                       attribute_key='run_log',
                       attribute_type=self.RUNLOG_ATTRIBUTE_TYPE,
                       attribute_value=json.dumps(run_log.dict(), ensure_ascii=True))  # pylint: disable=no-member

    def get_parameters(self, run_id, **kwargs):
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
        records = self.get_from_db(run_id=run_id, attribute_type=self.PARAMETER_ATTRIBUTE_TYPE)

        parameters = {}
        for record in records:
            parameters[record.attribute_key] = json.loads(record.attribute_value)

        return parameters

    def set_parameters(self, run_id, parameters, **kwargs):
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
        logger.info(f'{self.service_name} Getting parameters for : {run_id}')
        for key, value in parameters.items():
            self.update_db(run_id=run_id,
                           attribute_type=self.PARAMETER_ATTRIBUTE_TYPE,
                           attribute_key=key,
                           attribute_value=json.dumps(value)
                           )

    def get_step_log(self, internal_name, run_id, **kwargs):
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

        records = self.get_from_db(run_id=run_id, attribute_type=self.STEPLOG_ATTRIBUTE_TYPE,
                                   attribute_key=internal_name)
        if len(records) > 1:
            raise Exception(f'{self.service_name} Getting too many step logs for {internal_name}')

        if not records:
            raise exceptions.StepLogNotFoundError(run_id=run_id, name=internal_name)

        json_str = json.loads(records[0].attribute_value)
        return StepLog(**json_str)

    def add_step_log(self, step_log, run_id, **kwargs):
        """
        Add the step log in the run log as identifed by the run_id in the datastore

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
        self.update_db(run_id=run_id,
                       attribute_key=step_log.internal_name,
                       attribute_type=self.STEPLOG_ATTRIBUTE_TYPE,
                       attribute_value=json.dumps(step_log.dict(), ensure_ascii=True))  # pylint: disable=no-member

    def get_branch_log(self, internal_branch_name, run_id, **kwargs):
        # Should get the run_log
        # Should search for the branch log with the name
        logger.info(f'{self.service_name} Getting the Branch log: {internal_branch_name} of {run_id}')

        if not internal_branch_name:
            return self.get_run_log_by_id(run_id=run_id)

        records = self.get_from_db(run_id=run_id, attribute_type=self.BRANCHLOG_ATTRIBUTE_TYPE,
                                   attribute_key=internal_branch_name)
        if len(records) > 1:
            raise Exception(f'{self.service_name} Getting too many branch logs for {internal_branch_name}')

        if not records:
            raise exceptions.BranchLogNotFoundError(run_id=run_id, name=internal_branch_name)

        json_str = json.loads(records[0].attribute_value)
        return BranchLog(**json_str)

    def add_branch_log(self, branch_log, run_id, **kwargs):
        # This could in some occasions be the RunLog
        # Get the run log
        # Get the branch and step containing the branch
        # Add the branch to the step
        # Write the run_log
        if type(branch_log) == RunLog:
            self.put_run_log(run_log=branch_log)
            return
        logger.info(f'{self.service_name} Adding the Branch log to DB: {branch_log.internal_name}')
        self.update_db(run_id=run_id,
                       attribute_key=branch_log.internal_name,
                       attribute_type=self.BRANCHLOG_ATTRIBUTE_TYPE,
                       attribute_value=json.dumps(branch_log.dict(), ensure_ascii=True))
