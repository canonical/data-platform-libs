#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to Azure Storage provider charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import CharmBase, RelationJoinedEvent
from ops.main import main
from ops.model import ActiveStatus, WaitingStatus



from charms.data_platform_libs.v0.azure_storage import (
    AzureStorageRequires,
    StorageConnectionInfoChangedEvent,
    StorageConnectionInfoGoneEvent
)

logger = logging.getLogger(__name__)
FIRST_RELATION = "first-azure-storage-credentials"
SECOND_RELATION = "second-azure-storage-credentials"


class ApplicationAzureStorageCharm(CharmBase):
    """Application charm that connects to s3 provider charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        first_container_name = f'{self.app.name.replace("-", "_")}_first_container'

        self.first_azure_storage_requirer = AzureStorageRequires(self, FIRST_RELATION, container=first_container_name)
        self.framework.observe(
            self.first_azure_storage_requirer.on.storage_connection_info_changed, self._on_first_credential_changed
        )
        self.framework.observe(
            self.first_azure_storage_requirer.on.storage_connection_info_gone, self._on_first_credential_gone
        )
        self.framework.observe(
            self.on[FIRST_RELATION].relation_joined, self._on_first_relation_joined
        )

        second_container_name = f'{self.app.name.replace("-", "_")}_second_container'

        self.second_azure_storage_requirer = AzureStorageRequires(self, SECOND_RELATION, container=second_container_name)
        self.framework.observe(
            self.second_azure_storage_requirer.on.storage_connection_info_changed, self._on_second_credential_changed
        )
        self.framework.observe(
            self.second_azure_storage_requirer.on.storage_connection_info_gone, self._on_second_credential_gone
        )
        self.framework.observe(
            self.on[SECOND_RELATION].relation_joined, self._on_second_relation_joined
        )


    def _on_first_relation_joined(self, _: RelationJoinedEvent):
        """On first s3 credential relation joined."""
        self.unit.status = ActiveStatus()

    def _on_second_relation_joined(self, _: RelationJoinedEvent):
        """On second s3 credential relation joined."""
        self.unit.status = ActiveStatus()

    def _on_start(self, _) -> None:
        """Only sets an waiting status."""
        # self.unit.status = WaitingStatus("Waiting for relation")
        self.unit.status = WaitingStatus("Waiting for relation")

    # First credential changed events observers.
    def _on_first_credential_changed(self, event: StorageConnectionInfoChangedEvent) -> None:
        """Event triggered when Azure storage connection info was changed for this application."""
        # Retrieve the credentials using the charm library.
        if not event.relation:
            return
        connection_info = self.first_azure_storage_requirer.fetch_relation_data([event.relation.id])[event.relation.id]
        logger.info(
            f"First azure storage credentials: {connection_info}"
        )
        self.unit.status = ActiveStatus("Received azure storage connection info of the first relation")

    def _on_first_credential_gone(self, _: StorageConnectionInfoGoneEvent) -> None:
        """Event triggered when the azure storage credentials are gone."""
        logger.info("First relation azure storage credentials are no more valid.")

    # Second credential changed events observers.
    def _on_second_credential_changed(self, event: StorageConnectionInfoChangedEvent) -> None:
        """Event triggered when azure storage credential was created for this application."""
        # Retrieve the credentials using the charm library.
        if not event.relation:
            return
        connection_info = self.second_azure_storage_requirer.fetch_relation_data([event.relation.id])[event.relation.id]
        logger.info(
            f"Second azure storage credentials: {connection_info}"
        )
        self.unit.status = ActiveStatus("Received azure storage credentials of the second relation")

    def _on_second_credential_gone(self, _: StorageConnectionInfoGoneEvent) -> None:
        """Event triggered when the azure storage credentials are gone."""
        logger.info("Second relation azure storage credentials are no more valid.")
        self.unit.status = WaitingStatus("Waiting for relation")


if __name__ == "__main__":
    main(ApplicationAzureStorageCharm)
