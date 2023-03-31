#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""OpenSearch provider charm that accepts connections from application charms.

This charm is meant to be used only for testing the libraries in this repository.
"""

import logging
from typing import Dict, Optional

from charms.data_platform_libs.v0.data_interfaces import (
    IndexRequestedEvent,
    OpenSearchProvides,
)
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)

PEER = "opensearch-peers"
REL = "opensearch-client"


class OpenSearchCharm(CharmBase):
    """OpenSearch charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the OpenSearchProvides charm library.
        self.opensearch_provider = OpenSearchProvides(self, relation_name=REL)
        self.framework.observe(
            self.opensearch_provider.on.index_requested, self._on_index_requested
        )
        self.framework.observe(self.on[PEER].relation_joined, self._on_peer_relation_joined)

    def _on_peer_relation_joined(self, _):
        pass

    @property
    def app_peer_data(self) -> Dict:
        """Application peer relation data object."""
        relation = self.model.get_relation(PEER)
        if not relation:
            return {}

        return relation.data[self.app]

    def get_secret(self, scope: str, key: str) -> Optional[str]:
        """Get secret from the secret storage."""
        if scope == "app":
            return self.app_peer_data.get(key, None)
        else:
            raise RuntimeError("Unknown secret scope.")

    def set_secret(self, scope: str, key: str, value: Optional[str]) -> None:
        """Set secret in the secret storage."""
        if scope == "app":
            if not value:
                del self.app_peer_data[key]
                return
            self.app_peer_data.update({key: value})
        else:
            raise RuntimeError("Unknown secret scope.")

    def _on_start(self, _) -> None:
        """Only sets an active status."""
        self.unit.status = ActiveStatus("OpenSearch Ready!")

    def _on_index_requested(self, event: IndexRequestedEvent):
        """Handle the on_index_requested event."""
        self.unit.status = MaintenanceStatus("Creating connection")
        # retrieve index name from the requirer side
        index = event.index

        relation_id = event.relation.id

        username = "admin"
        password = "password"
        endpoints = "host1:port,host2:port"
        self.set_secret("app", "username", username)
        self.set_secret("app", "password", password)
        self.set_secret("app", "endpoints", endpoints)
        # set connection info in the databag relation
        self.opensearch_provider.set_endpoints(relation_id, endpoints)
        self.opensearch_provider.set_credentials(relation_id, username=username, password=password)
        self.opensearch_provider.set_tls_ca(relation_id, "Canonical")
        self.opensearch_provider.set_index(relation_id, index)
        self.unit.status = ActiveStatus(f"index: {index} granted!")

    def _on_reset_unit_status(self, event: ActionEvent):
        """Reset the status message of the unit."""
        self.unit.status = ActiveStatus()
        event.set_results({"Status": "Reset unit status message"})


if __name__ == "__main__":
    main(OpenSearchCharm)
