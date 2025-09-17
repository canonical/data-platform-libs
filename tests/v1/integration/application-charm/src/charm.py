#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import subprocess

from ops import Relation
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus
from pydantic import Field, SecretStr
from pydantic.types import _SecretBase

from charms.data_platform_libs.v1.data_interfaces import (
    ExtraSecretStr,
    KafkaRequestModel,
    KafkaResponseModel,
    RequirerCommonModel,
    RequirerDataContractV1,
    ResourceCreatedEvent,
    ResourceEndpointsChangedEvent,
    ResourceEntityCreatedEvent,
    ResourceProviderModel,
    ResourceRequirerEventHandler,
)

logger = logging.getLogger(__name__)

# Extra roles that this application needs when interacting with the database.
EXTRA_USER_ROLES = "SUPERUSER"
EXTRA_USER_ROLES_KAFKA = "producer,consumer"
EXTRA_USER_ROLES_OPENSEARCH = "admin,default"
CONSUMER_GROUP_PREFIX = "test-prefix"
BAD_URL = "http://badurl"


class ExtendedResponseModel(ResourceProviderModel):
    topsecret: ExtraSecretStr = Field(default=None)
    donttellanyone: ExtraSecretStr = Field(default=None)
    new_field_req: str | None = Field(default=None)
    new_field2_req: str | None = Field(default=None)


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.get_plugin_status_action, self._on_get_plugin_status)

        # Events related to the first database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f"{self.app.name.replace('-', '_')}_first_database_db"
        self.first_database = ResourceRequirerEventHandler(
            charm=self,
            relation_name="first-database-db",
            requests=[
                RequirerCommonModel(resource=database_name, extra_user_roles=EXTRA_USER_ROLES)
            ],
            response_model=ExtendedResponseModel,
        )
        self.first_database_roles = ResourceRequirerEventHandler(
            self,
            "first-database-roles",
            requests=[
                RequirerCommonModel(
                    resource=database_name, entity_type="USER", extra_user_roles=EXTRA_USER_ROLES
                )
            ],
            response_model=ExtendedResponseModel,
        )
        self.framework.observe(
            self.first_database.on.resource_created, self._on_first_database_created
        )
        self.framework.observe(
            self.first_database.on.endpoints_changed, self._on_first_database_endpoints_changed
        )
        self.framework.observe(
            self.first_database_roles.on.resource_entity_created,
            self._on_first_database_entity_created,
        )

        # Events related to the second database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f"{self.app.name.replace('-', '_')}_second_database_db"

        # Keeping the charm backwards compatible, for upgrades testing
        self.second_database = ResourceRequirerEventHandler(
            charm=self,
            relation_name="second-database-db",
            requests=[
                RequirerCommonModel(
                    resource=database_name,
                    extra_user_roles=EXTRA_USER_ROLES,
                    external_node_connectivity=True,
                )
            ],
            response_model=ExtendedResponseModel,
        )

        self.framework.observe(
            self.second_database.on.resource_created, self._on_second_database_created
        )
        self.framework.observe(
            self.second_database.on.endpoints_changed, self._on_second_database_endpoints_changed
        )

        # Multiple database clusters charm events (clusters/relations without alias).
        database_name = f"{self.app.name.replace('-', '_')}_multiple_database_clusters"
        self.database_clusters = ResourceRequirerEventHandler(
            charm=self,
            relation_name="multiple-database-clusters",
            requests=[
                RequirerCommonModel(resource=database_name, extra_user_roles=EXTRA_USER_ROLES)
            ],
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.database_clusters.on.resource_created, self._on_cluster_database_created
        )
        self.framework.observe(
            self.database_clusters.on.endpoints_changed,
            self._on_cluster_endpoints_changed,
        )

        # Multiple database clusters charm events (defined dynamically
        # in the database requires charm library, using the provided cluster/relation aliases).
        database_name = f"{self.app.name.replace('-', '_')}_aliased_multiple_database_clusters"
        cluster_aliases = ["cluster1", "cluster2"]  # Aliases for the multiple clusters/relations.
        self.aliased_database_clusters = ResourceRequirerEventHandler(
            charm=self,
            relation_name="aliased-multiple-database-clusters",
            requests=[
                RequirerCommonModel(resource=database_name, extra_user_roles=EXTRA_USER_ROLES)
            ],
            response_model=ResourceProviderModel,
            relation_aliases=cluster_aliases,
        )
        # Each database cluster will have its own events
        # with the name having the cluster/relation alias as the prefix.
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_resource_created,
            self._on_cluster1_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_endpoints_changed,
            self._on_cluster1_endpoints_changed,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_resource_created,
            self._on_cluster2_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_endpoints_changed,
            self._on_cluster2_endpoints_changed,
        )

        # Kafka events

        self.kafka = ResourceRequirerEventHandler(
            charm=self,
            relation_name="kafka-client-topic",
            requests=[
                KafkaRequestModel(
                    resource="test-topic",
                    extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                    consumer_group_prefix=CONSUMER_GROUP_PREFIX,
                )
            ],
            response_model=KafkaResponseModel,
        )

        self.kafka_split_pattern = ResourceRequirerEventHandler(
            self,
            relation_name="kafka-split-pattern-client",
            requests=[
                KafkaRequestModel(
                    resource="test-topic-split-pattern",
                    extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                    consumer_group_prefix=CONSUMER_GROUP_PREFIX,
                )
            ],
            response_model=KafkaResponseModel,
        )
        self.framework.observe(
            self.kafka_split_pattern.on.endpoints_changed,
            self._on_kafka_bootstrap_server_changed,
        )
        self.framework.observe(
            self.kafka_split_pattern.on.resource_created, self._on_kafka_topic_created
        )
        self.kafka_roles = ResourceRequirerEventHandler(
            charm=self,
            relation_name="kafka-client-roles",
            requests=[
                KafkaRequestModel(
                    resource="test-topic",
                    entity_type="USER",
                    extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                    consumer_group_prefix=CONSUMER_GROUP_PREFIX,
                )
            ],
            response_model=KafkaResponseModel,
        )
        self.framework.observe(
            self.kafka_roles.on.resource_entity_created,
            self._on_kafka_entity_created,
        )

        self.framework.observe(
            self.kafka.on.endpoints_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(self.kafka.on.resource_created, self._on_kafka_topic_created)

        # Kafka Connect events

        self.connect_source = ResourceRequirerEventHandler(
            self,
            "connect-source",
            requests=[RequirerCommonModel(resource="http://10.10.10.10:8000")],
            response_model=ResourceProviderModel,
        )
        self.connect_sink = ResourceRequirerEventHandler(
            self,
            "connect-sink",
            requests=[RequirerCommonModel(resource=BAD_URL)],
            response_model=ResourceProviderModel,
        )

        self.framework.observe(
            self.connect_source.on.resource_created, self._on_connect_integration_created
        )

        self.framework.observe(
            self.connect_source.on.endpoints_changed,
            self._on_connect_endpoints_changed,
        )

        # OpenSearch events

        self.opensearch = ResourceRequirerEventHandler(
            charm=self,
            relation_name="opensearch-client-index",
            requests=[
                RequirerCommonModel(
                    resource="test-index", extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH
                )
            ],
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.opensearch.on.resource_created, self._on_opensearch_index_created
        )

        self.framework.observe(
            self.opensearch.on.authentication_updated, self._on_opensearch_authentication_updated
        )

        self.opensearch_roles = ResourceRequirerEventHandler(
            charm=self,
            relation_name="opensearch-client-roles",
            requests=[
                RequirerCommonModel(
                    resource="test-index",
                    entity_type="USER",
                    extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH,
                )
            ],
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.opensearch_roles.on.resource_entity_created,
            self._on_opensearch_entity_created,
        )

        # actions

        self.framework.observe(self.on.reset_unit_status_action, self._on_reset_unit_status)
        self.framework.observe(self.on.set_mtls_cert_action, self._on_set_mtls_cert)

        # Get/set/delete fields on second-database relations
        self.framework.observe(self.on.get_relation_field_action, self._on_get_relation_field)
        self.framework.observe(
            self.on.get_relation_self_side_field_action, self._on_get_relation_self_side_field
        )
        self.framework.observe(self.on.set_relation_field_action, self._on_set_relation_field)
        self.framework.observe(
            self.on.delete_relation_field_action, self._on_delete_relation_field
        )
        self._relation_endpoints: list[ResourceRequirerEventHandler] = [
            self.first_database,
            self.second_database,
            self.database_clusters,
            self.aliased_database_clusters,
            self.kafka,
            self.kafka_roles,
            self.connect_source,
            self.connect_sink,
            self.opensearch,
            self.opensearch_roles,
        ]

    def _get_relation(self, relation_id: int) -> tuple[ResourceRequirerEventHandler, Relation]:
        """Retrieve a relation by ID, together with the corresponding endpoint object ('Requires')."""
        for source in self._relation_endpoints:
            for relation in source.relations:
                if relation.id == relation_id:
                    return (source, relation)
        raise ValueError(f"Invalid relation id {relation_id}")

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    # Generic relation actions
    def _on_get_relation_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        value = None
        model = source.interface.build_model(relation.id, component=relation.app)
        for request in model.requests:
            value = getattr(request, event.params["field"].replace("-", "_"))
        value = value.get_secret_value() if issubclass(value.__class__, _SecretBase) else value
        event.set_results({"value": value if value else ""})

    def _on_get_relation_self_side_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        value = source.interface.repository(relation.id).get_field(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_set_relation_field(self, event: ActionEvent):
        """Set requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        source.interface.repository(relation.id).write_field(
            event.params["field"], event.params["value"]
        )

    def _on_delete_relation_field(self, event: ActionEvent):
        """Delete requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        source.interface.repository(relation.id).delete_field(event.params["field"])

    # First database events observers.
    def _on_first_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"first database credentials: {event.response.username} {event.response.password}"
        )
        self.unit.status = ActiveStatus("received database credentials of the first database")

    def _on_first_database_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"first database endpoints have been changed to: {event.response.endpoints}")

    def _on_first_database_entity_created(self, event: ResourceEntityCreatedEvent) -> None:
        """Event triggered when a database entity was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"first database entity credentials: {event.response.entity_name}")
        self.unit.status = ActiveStatus("received entity credentials of the first database")

    # Second database events observers.
    def _on_second_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"second database credentials: {event.response.username} {event.response.password}"
        )
        self.unit.status = ActiveStatus("received database credentials of the second database")

    def _on_second_database_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"second database endpoints have been changed to: {event.response.endpoints}")

    # Multiple database clusters events observers.
    def _on_cluster_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"cluster {event.relation.app.name} credentials: {event.response.username} {event.response.password}"
        )
        self.unit.status = ActiveStatus(
            f"received database credentials for cluster {event.relation.app.name}"
        )

    def _on_cluster_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(
            f"cluster {event.relation.app.name} endpoints have been changed to: {event.response.endpoints}"
        )

    # Multiple database clusters events observers (for aliased clusters/relations).
    def _on_cluster1_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster1 credentials: {event.response.username} {event.response.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster1")

    def _on_cluster1_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster1 endpoints have been changed to: {event.response.endpoints}")

    def _on_cluster2_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster2 credentials: {event.response.username} {event.response.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster2")

    def _on_cluster2_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster2 endpoints have been changed to: {event.response.endpoints}")

    def _on_get_plugin_status(self, event: ActionEvent) -> None:
        """Returns the PostgreSQL plugin status (enabled/disabled)."""
        plugin = event.params.get("plugin")
        if not plugin:
            event.fail("Please provide a plugin name")
            return

        plugin_status = (
            "enabled" if self.first_database.is_postgresql_plugin_enabled(plugin) else "disabled"
        )
        event.set_results({"plugin-status": plugin_status})

    def _on_kafka_bootstrap_server_changed(self, event: ResourceEndpointsChangedEvent):
        """Event triggered when a bootstrap server was changed for this application."""
        logger.info(
            f"On kafka boostrap-server changed: bootstrap-server: {event.response.endpoints}"
        )
        self.unit.status = ActiveStatus("kafka_bootstrap_server_changed")

    def _on_kafka_topic_created(self, _: ResourceCreatedEvent):
        """Event triggered when a topic was created for this application."""
        logger.info("On kafka topic created")
        self.unit.status = ActiveStatus("kafka_topic_created")

    def _on_kafka_entity_created(self, _: ResourceEntityCreatedEvent) -> None:
        """Event triggered when a topic entity was created for this application."""
        logger.info("On kafka entity created")
        self.unit.status = ActiveStatus("kafka_entity_created")

    def _on_connect_integration_created(self, _: ResourceCreatedEvent):
        """Event triggered when Kafka Connect integration credentials are created for this application."""
        self.unit.status = ActiveStatus("connect_integration_created")

    def _on_connect_endpoints_changed(self, _: ResourceEndpointsChangedEvent):
        """Event triggered when Kafka Connect REST endpoints change."""
        self.unit.status = ActiveStatus("connect_endpoints_changed")

    def _on_opensearch_index_created(self, _: ResourceCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch index created event fired")
        self.unit.status = ActiveStatus("opensearch_index_created")

    def _on_opensearch_entity_created(self, _: ResourceEntityCreatedEvent):
        """Event triggered when an index entity was created for this application."""
        logger.info("On opensearch entity created event fired")
        self.unit.status = ActiveStatus("opensearch_entity_created")

    def _on_opensearch_authentication_updated(self, _: ResourceCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch authentication_updated event fired")
        self.unit.status = ActiveStatus("opensearch_authentication_updated")

    def _on_reset_unit_status(self, _: ActionEvent):
        """Handle the reset of status message for the unit."""
        self.unit.status = ActiveStatus()

    def _on_set_mtls_cert(self, event: ActionEvent):
        """Sets the MTLS cert for the relation."""
        cmd = f'openssl req -new -newkey rsa:2048 -days 365 -nodes -subj "/CN={self.unit.name.replace("/", "-")}" -x509 -keyout client.key -out client.pem'
        subprocess.check_output(cmd, shell=True, universal_newlines=True)
        cert = open("./client.pem", "r").read()
        relation = self.model.get_relation("kafka-split-pattern-client")
        assert relation
        model = self.kafka_split_pattern.interface.build_model(
            relation.id, RequirerDataContractV1[KafkaRequestModel], component=self.app
        )
        for response in model.requests:
            response.mtls_cert = SecretStr(cert)
        self.kafka_split_pattern.interface.write_model(relation.id, model)
        event.set_results({"mtls-cert": cert})


if __name__ == "__main__":
    main(ApplicationCharm)
