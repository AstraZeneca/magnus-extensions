import logging

from magnus import defaults
from magnus.integration import BaseIntegration

logger = logging.getLogger(defaults.NAME)


class BufferedRunLogStore(BaseIntegration):
    """
    Only local execution mode is possible for Buffered Run Log store
    """

    executor_type = "slurm-job"
    service_type = "run_log_store"  # One of secret, catalog, datastore
    service_provider = "buffered"  # The actual implementation of the service

    def validate(self, **kwargs):
        raise Exception("Slurm job cannot run work with buffered run log store")
