import logging
from pathlib import Path

from magnus import defaults
from magnus.datastore import ChunkedFileSystemRunLogStore

logger = logging.getLogger(defaults.NAME)


class ChunkedK8PersistentVolumeRunLogstore(ChunkedFileSystemRunLogStore):
    """
    Uses the K8s Persistent Volumes to store run logs.
    """

    service_name = "chunked-k8s-pvc"

    class Config(ChunkedFileSystemRunLogStore.Config):
        persistent_volume_name: str
        mount_path: str

    @property
    def log_folder_name(self) -> str:
        return str(Path(self.config.mount_path) / self.config.log_folder)
