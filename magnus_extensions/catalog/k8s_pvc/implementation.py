import logging
from pathlib import Path

from magnus import defaults
from magnus.catalog import FileSystemCatalog

logger = logging.getLogger(defaults.NAME)


class K8sPVCatalog(FileSystemCatalog):

    service_name = 'k8s-pvc'

    class Config(FileSystemCatalog.Config):
        persistent_volume_name: str
        mount_path: str

    @property
    def catalog_location(self) -> str:
        """
        Get the catalog location from the config.
        If its not defined, use the magnus default

        Returns:
            str: The catalog location as defined by the config or magnus default '.catalog'
        """
        return str(Path(self.config.mount_path) / self.config.catalog_location)  # type: ignore
