#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""OpenSearch provider charm that accepts connections from application charms.

This charm is meant to be used only for testing the libraries in this repository.
"""

import logging
import secrets
import string

from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus
from pydantic import SecretStr

from charms.data_platform_libs.v1.data_interfaces import (
    DataContractV1,
    RequirerCommonModel,
    ResourceEntityRequestedEvent,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    ResourceRequestedEvent,
    SecretBool,
)

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
        self.opensearch_provider = ResourceProviderEventHandler(self, REL, RequirerCommonModel)
        self.framework.observe(
            self.opensearch_provider.on.resource_requested,
            self._on_index_requested,
        )
        self.framework.observe(
            self.opensearch_provider.on.resource_entity_requested,
            self._on_index_entity_requested,
        )
        self.framework.observe(self.on[PEER].relation_joined, self._on_peer_relation_joined)
        self.framework.observe(
            self.on.change_admin_password_action, self._on_change_admin_password
        )

    def _on_peer_relation_joined(self, _):
        pass

    @property
    def app_peer_data(self) -> dict:
        """Application peer relation data object."""
        relation = self.model.get_relation(PEER)
        if not relation:
            return {}

        return relation.data[self.app]

    def get_secret(self, scope: str, key: str) -> str | None:
        """Get secret from the secret storage."""
        if scope == "app":
            return self.app_peer_data.get(key, None)
        else:
            raise RuntimeError("Unknown secret scope.")

    def set_secret(self, scope: str, key: str, value: str | None) -> None:
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

    def _on_change_admin_password(self, event: ActionEvent):
        """Change the admin password."""
        password = self._new_password()
        for relation in self.opensearch_provider.relations:
            model = self.opensearch_provider.interface.build_model(
                relation.id, DataContractV1[ResourceProviderModel]
            )
            for request in model.requests:
                request.password = SecretStr(password)
            self.opensearch_provider.interface.write_model(relation.id, model)

    def _on_index_requested(self, event: ResourceRequestedEvent[RequirerCommonModel]):
        """Handle the on_index_requested event."""
        self.unit.status = MaintenanceStatus("Creating connection")
        # retrieve index name from the requirer side
        index = event.request.resource

        relation_id = event.relation.id

        username = "admin"
        password = "password"
        endpoints = "host1:port,host2:port"
        self.set_secret("app", "username", username)
        self.set_secret("app", "password", password)
        self.set_secret("app", "endpoints", endpoints)
        # set connection info in the databag relation
        response = ResourceProviderModel(
            salt=event.request.salt,
            request_id=event.request.request_id,
            resource=index,
            username=SecretStr(username),
            password=SecretStr(password),
            tls_ca=SecretStr("Canonical"),
            endpoints=endpoints,
            tls=SecretBool(True),
        )
        self.opensearch_provider.set_response(relation_id, response)
        self.unit.status = ActiveStatus(f"index: {index} granted!")

    def _on_index_entity_requested(self, event: ResourceEntityRequestedEvent):
        """Handle the on_index_entity_requested event."""
        self.unit.status = MaintenanceStatus("Creating entity")

        rolename = "admin"
        password = "password"
        self.set_secret("app", "entity-name", rolename)
        self.set_secret("app", "entity-password", password)
        response = ResourceProviderModel(
            salt=event.request.salt,
            request_id=event.request.request_id,
            entity_name=SecretStr(rolename),
            entity_password=SecretStr(password),
        )
        # set connection info in the databag relation
        self.opensearch_provider.set_response(event.relation.id, response)
        self.unit.status = ActiveStatus(f"entity: {rolename} created!")

    def _on_reset_unit_status(self, event: ActionEvent):
        """Reset the status message of the unit."""
        self.unit.status = ActiveStatus()
        event.set_results({"Status": "Reset unit status message"})

    def _new_password(self) -> str:
        """Generate a random password string.

        Returns:
           A random password string.
        """
        choices = string.ascii_letters + string.digits
        return "".join([secrets.choice(choices) for i in range(16)])


if __name__ == "__main__":
    main(OpenSearchCharm)
