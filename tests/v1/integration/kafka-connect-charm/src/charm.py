#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kafka Connect provider charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

from charms.data_platform_libs.v1.data_interfaces import (
    DataContractV1,
    RequirerCommonModel,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    ResourceRequestedEvent,
)

logger = logging.getLogger(__name__)

PEER = "worker"
REL = "connect-client"
BAD_URL = "http://badurl"
SYNC_ACTIONS = ("username", "password", "endpoints")


class KafkaConnectCharm(CharmBase):
    """Kafka connect charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the Kafka Connect Provides charm library.
        self.provider = ResourceProviderEventHandler(self, REL, RequirerCommonModel)
        self.framework.observe(self.provider.on.resource_requested, self._on_integration_requested)
        self.framework.observe(self.on[PEER].relation_joined, self._on_peer_relation_joined)

        # syncaction
        self.framework.observe(self.on.sync_action, self._on_sync)

    def _on_peer_relation_joined(self, _):
        pass

    @property
    def app_peer_data(self) -> dict:
        """Application peer relation data object."""
        relation = self.model.get_relation(PEER)
        if not relation:
            return {}

        return relation.data[self.app]

    def get_secret(self, scope: str, key: str) -> str:
        """Get secret from the secret storage."""
        if scope == "app":
            return self.app_peer_data.get(key, "")
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
        self.unit.status = ActiveStatus("Kafka Connect Ready!")

    def _download_plugin(self, plugin_url) -> bool:
        """Fake plugin downloader, returns True on every URL except `BAD_URL`."""
        if plugin_url == BAD_URL:
            return False

        return True

    def _on_integration_requested(self, event: ResourceRequestedEvent):
        """Handle the `on_integration_requested` event."""
        # retrieve `plugin-url` from the requirer side
        plugin_url = event.request.resource
        self.unit.status = MaintenanceStatus(f"Retrieving plugin from client: {plugin_url}.")

        if not self._download_plugin(plugin_url):
            event.defer()
            return

        self.unit.status = MaintenanceStatus("Plugin downloaded successfully.")
        relation_id = event.relation.id

        username = "integrator"
        password = "password"
        endpoints = "http://worker1:8083,http://worker2:8083"
        self.set_secret("app", "username", username)
        self.set_secret("app", "password", password)
        self.set_secret("app", "endpoints", endpoints)
        # set connection info in the databag relation
        response = ResourceProviderModel(
            salt=event.request.salt,
            request_id=event.request.request_id,
            endpoints=endpoints,
            username=username,
            password=password,
            tls=False,
            tls_ca="disabled",
        )
        self.provider.set_response(relation_id, response)
        self.unit.status = ActiveStatus(
            f"Integration setup successful for relation {relation_id}!"
        )

    def _on_sync(self, event: ActionEvent):
        """Handler for `sync` action."""
        key = event.params.get("key")
        if key not in SYNC_ACTIONS:
            event.fail(f"Action '{key}' not permitted.")
            return

        value = event.params.get("value", "")
        self.set_secret("app", key, value)

        # update clients data
        if len(self.provider.relations) > 0:
            self._update_clients_data(key, value)

        event.set_results({key: value})
        self.unit.status = ActiveStatus(f"{key} changed on connect_client!")

    def _update_clients_data(self, key: str, value: str) -> None:
        """Updates connect clients data."""
        if key not in ("username", "password", "endpoints"):
            return
        for relation in self.provider.relations:
            model = self.provider.interface.build_model(
                relation.id, DataContractV1[ResourceProviderModel]
            )
            for request in model.requests:
                if key in ("username", "password"):
                    setattr(request, key, self.get_secret("app", key))
                else:
                    setattr(request, key, value)
            self.provider.interface.write_model(relation.id, model)

    def _on_reset_unit_status(self, event: ActionEvent):
        """Reset the status message of the unit."""
        self.unit.status = ActiveStatus()
        event.set_results({"Status": "Reset unit status message"})


if __name__ == "__main__":
    main(KafkaConnectCharm)
