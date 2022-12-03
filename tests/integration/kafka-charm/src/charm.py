#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kafka provider charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
from typing import Dict, Optional

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaProvides,
    TopicRequestedEvent,
)
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)

PEER = "kafka-peers"
REL = "kafka-client"


class KafkaCharm(CharmBase):
    """Kafka charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the Kafka Provides charm library.
        self.kafka_provider = KafkaProvides(self, relation_name=REL)
        self.framework.observe(self.kafka_provider.on.topic_requested, self._on_topic_requested)
        self.framework.observe(self.on[PEER].relation_joined, self._on_peer_relation_joined)

        # actions
        self.framework.observe(self.on.sync_password_action, self._on_sync_password)
        self.framework.observe(self.on.sync_username_action, self._on_sync_username)
        self.framework.observe(
            self.on.sync_bootstrap_server_action, self._on_sync_bootstrap_server
        )

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
        self.unit.status = ActiveStatus("Kakfa Ready!")

    def _on_topic_requested(self, event: TopicRequestedEvent):
        """Handle the on_topic_requested event."""
        self.unit.status = MaintenanceStatus("Creating connection")
        # retrieve topic name from the requirer side
        topic = event.topic

        relation_id = event.relation.id

        username = "admin"
        password = "password"
        bootstrap_server = "host1:port,host2:port"
        self.set_secret("app", "username", username)
        self.set_secret("app", "password", password)
        self.set_secret("app", "bootstrap-server", bootstrap_server)
        # set connection info in the databag relation
        self.kafka_provider.set_bootstrap_server(relation_id, bootstrap_server)
        self.kafka_provider.set_credentials(relation_id, username=username, password=password)
        self.kafka_provider.set_consumer_group_prefix(relation_id, "group1,group2")
        self.kafka_provider.set_tls(relation_id, "True")
        self.kafka_provider.set_tls_ca(relation_id, "Canonical")
        self.kafka_provider.set_zookeeper_uris(relation_id, "protocol.z1:port/,protocol.z2:port/")

        self.unit.status = ActiveStatus(f"Topic: {topic} granted!")

    def _on_sync_password(self, event: ActionEvent):
        """Set the password in the data relation databag."""
        logger.info("On sync password")
        password = event.params["password"]
        self.set_secret("app", "password", password)
        logger.info(f"New password: {password}")
        # set parameters in the secrets
        # update relation data if the relation is present
        if len(self.kafka_provider.relations) > 0:
            for relation in self.kafka_provider.relations:
                self.kafka_provider.set_credentials(
                    relation.id,
                    username=self.get_secret("app", "username"),
                    password=self.get_secret("app", "password"),
                )
        event.set_results({"password": self.get_secret("app", "password")})

    def _on_sync_username(self, event: ActionEvent):
        """Set the username in the data relation databag."""
        username = event.params["username"]
        self.set_secret("app", "username", username)
        # set parameters in the secrets
        # update relation data if the relation is present
        if len(self.kafka_provider.relations) > 0:
            for relation in self.kafka_provider.relations:
                self.kafka_provider.set_credentials(
                    relation.id,
                    username=self.get_secret("app", "username"),
                    password=self.get_secret("app", "password"),
                )
        event.set_results({"username": self.get_secret("app", "username")})

    def _on_sync_bootstrap_server(self, event: ActionEvent):
        """Set the bootstrap server in the data relation databag."""
        bootstrap_server = event.params["bootstrap-server"]
        self.set_secret("app", "bootstrap-server", bootstrap_server)
        # set parameters in the secrets
        # update relation data if the relation is present
        if len(self.kafka_provider.relations) > 0:
            for relation in self.kafka_provider.relations:
                self.kafka_provider.set_bootstrap_server(
                    relation.id, self.get_secret("app", "bootstrap-server")
                )
        event.set_results({"bootstrap-server": self.get_secret("app", "bootstrap-server")})

    def _on_reset_unit_status(self, event: ActionEvent):
        """Reset the status message of the unit."""
        self.unit.status = ActiveStatus()
        event.set_results({"Status": "Reset unit status message"})


if __name__ == "__main__":
    main(KafkaCharm)
