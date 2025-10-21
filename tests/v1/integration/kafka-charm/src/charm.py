#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kafka provider charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import os

from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

from charms.data_platform_libs.v1.data_interfaces import (
    DataContractV1,
    KafkaRequestModel,
    KafkaResponseModel,
    MtlsCertUpdatedEvent,
    ResourceEntityRequestedEvent,
    ResourceProviderEventHandler,
    ResourceRequestedEvent,
)

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
        self.kafka_provider = ResourceProviderEventHandler(
            self, relation_name=REL, request_model=KafkaRequestModel, mtls_enabled=True
        )
        self.framework.observe(
            self.kafka_provider.on.resource_requested,
            self._on_topic_requested,
        )
        self.framework.observe(
            self.kafka_provider.on.resource_entity_requested,
            self._on_topic_entity_requested,
        )

        self.framework.observe(self.on[PEER].relation_joined, self._on_peer_relation_joined)

        # actions
        self.framework.observe(self.on.sync_password_action, self._on_sync_password)
        self.framework.observe(self.on.sync_username_action, self._on_sync_username)
        self.framework.observe(
            self.on.sync_bootstrap_server_action, self._on_sync_bootstrap_server
        )
        self.framework.observe(
            self.kafka_provider.on.mtls_cert_updated, self._on_mtls_cert_updated
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
        self.unit.status = ActiveStatus("Kafka Ready!")

    def _on_topic_requested(self, event: ResourceRequestedEvent[KafkaRequestModel]):
        """Handle the on_topic_requested event."""
        self.unit.status = MaintenanceStatus("Creating connection")
        # retrieve topic name from the requirer side
        topic = event.request.resource
        consumer_group_prefix = event.request.consumer_group_prefix

        relation_id = event.relation.id

        username = "admin"
        password = "password"
        bootstrap_server = "host1:port,host2:port"
        self.set_secret("app", "username", username)
        self.set_secret("app", "password", password)
        self.set_secret("app", "bootstrap-server", bootstrap_server)
        # set connection info in the databag relation
        response = KafkaResponseModel(
            salt=event.request.salt,
            request_id=event.request.request_id,
            username=username,
            password=password,
            endpoints=bootstrap_server,
            consumer_group_prefix=consumer_group_prefix,
            tls=True,
            tls_ca="Canonical",
            zookeeper_uris="protocol.z1:port/,protocol.z2:port/",
            resource=topic,
        )
        self.kafka_provider.set_response(relation_id, response)
        self.unit.status = ActiveStatus(f"Topic: {topic} granted!")

    def _on_topic_entity_requested(self, event: ResourceEntityRequestedEvent):
        """Handle the on_topic_entity_requested event."""
        self.unit.status = MaintenanceStatus("Creating entity")

        rolename = "admin"
        password = "password"
        self.set_secret("app", "entity-name", rolename)
        self.set_secret("app", "entity-password", password)
        response = KafkaResponseModel(
            request_id=event.request.request_id,
            salt=event.request.salt,
            entity_name=rolename,
            entity_password=password,
        )
        # set connection info in the databag relation
        self.kafka_provider.set_response(event.relation.id, response)
        self.unit.status = ActiveStatus(f"Entity: {rolename} created!")

    def _on_sync_password(self, event: ActionEvent):
        """Set the password in the data relation databag."""
        logger.info("On sync password")

        password: str | None = event.params.get("password")
        if not password:
            event.fail("Invalid password")
            return
        self.set_secret("app", "password", password)
        logger.info(f"New password: {password}")
        # set parameters in the secrets
        # update relation data if the relation is present
        if len(self.kafka_provider.relations) > 0:
            for relation in self.kafka_provider.relations:
                model = self.kafka_provider.interface.build_model(
                    relation.id, DataContractV1[KafkaResponseModel]
                )
                for request in model.requests:
                    request.password = password
                self.kafka_provider.interface.write_model(relation.id, model)
        event.set_results({"password": self.get_secret("app", "password")})

    def _on_sync_username(self, event: ActionEvent):
        """Set the username in the data relation databag."""
        username = event.params["username"]

        if not username:
            event.fail("Invalid username")
            return

        self.set_secret("app", "username", username)

        # set parameters in the secrets
        # update relation data if the relation is present
        if len(self.kafka_provider.relations) > 0:
            for relation in self.kafka_provider.relations:
                model = self.kafka_provider.interface.build_model(
                    relation.id, DataContractV1[KafkaResponseModel]
                )
                for request in model.requests:
                    request.username = username
                self.kafka_provider.interface.write_model(relation.id, model)
        event.set_results({"username": self.get_secret("app", "username")})

    def _on_sync_bootstrap_server(self, event: ActionEvent):
        """Set the bootstrap server in the data relation databag."""
        bootstrap_server = event.params["bootstrap-server"]
        self.set_secret("app", "bootstrap-server", bootstrap_server)
        # set parameters in the secrets
        # update relation data if the relation is present
        if len(self.kafka_provider.relations) > 0:
            for relation in self.kafka_provider.relations:
                model = self.kafka_provider.interface.build_model(
                    relation.id, DataContractV1[KafkaResponseModel]
                )
                for request in model.requests:
                    request.endpoints = self.get_secret("app", "bootstrap-server")
                self.kafka_provider.interface.write_model(relation.id, model)
        event.set_results({"bootstrap-server": self.get_secret("app", "bootstrap-server")})

    def _on_reset_unit_status(self, event: ActionEvent):
        """Reset the status message of the unit."""
        self.unit.status = ActiveStatus()
        event.set_results({"Status": "Reset unit status message"})

    def _on_mtls_cert_updated(self, event: MtlsCertUpdatedEvent):
        mtls_cert = event.request.mtls_cert
        if not mtls_cert:
            return

        open("client-cert.pem", "w").write(mtls_cert)
        self.unit.status = ActiveStatus(f"{os.getcwd()}/client-cert.pem")


if __name__ == "__main__":
    main(KafkaCharm)
