#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""S3 provider charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import CharmBase, ConfigChangedEvent
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

from charms.data_platform_libs.v0.azure_storage import (
    AzureStorageProvides,
    StorageConnectionInfoRequestedEvent
)

logger = logging.getLogger(__name__)

RELATION_NAME = "azure-storage-credentials"

class AzureStorageCharm(CharmBase):
    """S3 charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the s3 provides charm library.
        self.azure_storage_provider = AzureStorageProvides(self, relation_name=RELATION_NAME)
        self.framework.observe(
            self.azure_storage_provider.on.storage_connection_info_requested, self._on_connection_info_requested
        )
        self.framework.observe(self.on.config_changed, self._on_config_changed)

    def _on_start(self, _) -> None:
        """Only sets an waiting status."""
        # self.unit.status = WaitingStatus("Waiting for relation")
        self.unit.status = ActiveStatus()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        # Only execute in the unit leader
        if not self.unit.is_leader():
            return
        
        data = dict(self.config)

        logger.info(f"Config changed: {data}")

        relation = self.model.get_relation(relation_name=RELATION_NAME)
        if not relation:
            return

        self.azure_storage_provider.relation_data.update_relation_data(relation_id=relation.id, data=data)

    def _on_connection_info_requested(self, event: StorageConnectionInfoRequestedEvent):
        self.unit.status = MaintenanceStatus("Preparing credentials..")
        container = event.container

        relation_id = event.relation.id

        connection_info = dict(self.config)
        # set connection info in the databag relation
        self.azure_storage_provider.relation_data.update_relation_data(relation_id=relation_id, data=connection_info)

        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(AzureStorageCharm)
