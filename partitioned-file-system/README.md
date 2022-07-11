# DB Run log store provider

This package is an extension to [magnus](https://github.com/AstraZeneca/magnus-core).

## Provides 
Provides capability to have a database as a run log store.

This run log store is concurrent safe.

## Installation instructions

```pip install magnus_extension_datastore_db```

## Set up required to use the extension

A database schema and a role with read/write privileges.

The DB model used by this extension is:

```python
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
```

Please note that ```created_at``` is important for ordering of the steps and events and should be always increasing for
new instances (records).

You can either create this schema using your own mechanisms or can use the handy script provided as part of this
package.

```python
from magnus_extension_datastore_db import db
db.create_tables(<connection_string>)
```

## Config parameters

The full configuration of this run log store is:

```yaml
run_log:
  type: db
  config:
    connection_string: The connection string to use in SQLAlchemy. Secret placeholders are fine.
```

### **connection_string**:

Please provide the connection string of the database using this variable.

You can use placeholders for sensitive details and provide it by the secrets manager. Internally, we use 
[python template strings](https://docs.python.org/3/library/string.html#template-strings) 
to create a template and 
[safe substitute](https://docs.python.org/3/library/string.html#string.Template.safe_substitute) with secrets 
key value pairs.

For example, a connection string ```'postgresql://scott:${password}@localhost:5432/mydatabase'``` and secrets having
a key value pair of ```password=tiger``` would result in a connection string of
```'postgresql://scott:tiger@localhost:5432/mydatabase'```