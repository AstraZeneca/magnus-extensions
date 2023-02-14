# Welcome to Magnus Extensions

Documentation of the extensions are available at: https://astrazeneca.github.io/magnus-extensions/

This repository provides all the extensions to [magnus core package](https://github.com/AstraZeneca/magnus-core).

Magnus provides 5 essential services:

- Executor: A way to define and execute/transpile dag definition.
- Catalog: An artifact store used to log and store data files generated during a pipeline execution.
- Secrets Handler: A framework to handle secrets from different providers.
- Logging: A comprehensive and automatic logging framework to capture essential information of a pipeline execution.
- Experiment Tracking: A framework to interact with different experiment tracking tools.

Below is the table of all the available extensions to the above services:

| Service     | Description                          |   Availability   |
| :---------: | :----------------------------------: |  :-------------: |
| **Executors**   |                                      |                  |   
| Local       | Run the pipeline on local machine (default) |   Part of Magnus core |
| Local Containers    | Run the pipeline on local containers | Part of Magnus core |
| **Catalog**     |                                      |                  |
| Do Nothing  | Provides no cataloging functionality |   Part of Magnus core |
| File System  | Uses local file system (default) |   Part of Magnus core |
| S3 | Uses S3 as a catalog | magnus_extension_catalog_s3 |
| **Secrets**     |                                      |                  |
| Do Nothing  | Provides no secrets handler (default) |   Part of Magnus core |
| Dot Env  | Uses a file as secrets  |   Part of Magnus core |
| Environment Variables  | Gets secrets from Environmental variables  |   Part of Magnus core |
| **Logging**     |                                      |                  |
|   Buffered  | Uses the run time buffer as logger (default) |   Part of Magnus core |
| File System  | Uses a file system as run log store  |   Part of Magnus core |
| S3 | Uses S3 to store logs | magnus_extension_datastore_s3 |
| **Experiment Tracking**     |                                      |                  |
|   Do Nothing  | Provides no experiment tracking (default) |   Part of Magnus core |
