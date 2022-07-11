import logging
import json
from string import Template as str_template
import datetime
from collections import OrderedDict


from magnus.datastore import BaseRunLogStore, RunLog, StepLog, BranchLog
from magnus import defaults
from magnus import exceptions

from magnus_extension_datastore_db.base import PartitionDataStore

logger = logging.getLogger(defaults.NAME)
logger.info('Loading DB datastore extension')


try:
    import sqlalchemy
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column, Text, DateTime, Sequence, Integer

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


class DBStore(PartitionDataStore):
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
            from magnus.pipeline import global_executor  # pylint: disable=C0415

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

    def _persist(self, run_id: str, attribute_type: str, attribute_key: str, attribute_value: str):
        """
        Update the Database with a part of run log.


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

            if len(records) == 0:
                # We insert otherwise
                record = DBLog(run_id=run_id, attribute_key=attribute_key,
                               attribute_type=attribute_type, attribute_value=attribute_value)
                session.add(record)

            session.commit()

    def _retrieve(self, run_id: str, attribute_type: str, attribute_key=None):
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
