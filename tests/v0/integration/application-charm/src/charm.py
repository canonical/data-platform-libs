#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import subprocess
from typing import Optional, Tuple

from ops import Relation
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus

from charms.data_platform_libs.v0.data_interfaces import (
    LIBPATCH as DATA_INTERFACES_VERSION,
)
from charms.data_platform_libs.v0.data_interfaces import (
    BootstrapServerChangedEvent,
    DatabaseCreatedEvent,
    DatabaseEndpointsChangedEvent,
    DatabaseRequires,
    IndexCreatedEvent,
    KafkaRequires,
    OpenSearchRequires,
    TopicCreatedEvent,
)

if DATA_INTERFACES_VERSION > 34:
    from charms.data_platform_libs.v0.data_interfaces import (
        KafkaRequirerData,
        KafkaRequirerEventHandlers,
    )

if DATA_INTERFACES_VERSION > 49:
    from charms.data_platform_libs.v0.data_interfaces import (
        ENTITY_USER,
        DatabaseEntityCreatedEvent,
        IndexEntityCreatedEvent,
        TopicEntityCreatedEvent,
    )

if DATA_INTERFACES_VERSION > 52:
    from charms.data_platform_libs.v0.data_interfaces import (
        IntegrationCreatedEvent,
        IntegrationEndpointsChangedEvent,
        KafkaConnectRequires,
    )


logger = logging.getLogger(__name__)

# Extra roles that this application needs when interacting with the database.
EXTRA_USER_ROLES = "SUPERUSER"
EXTRA_USER_ROLES_KAFKA = "producer,consumer"
EXTRA_USER_ROLES_OPENSEARCH = "admin,default"
CONSUMER_GROUP_PREFIX = "test-prefix"
BAD_URL = "http://badurl"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.get_plugin_status_action, self._on_get_plugin_status)

        # Events related to the first database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f'{self.app.name.replace("-", "_")}_first_database_db'
        self.first_database = DatabaseRequires(
            charm=self,
            relation_name="first-database-db",
            database_name=database_name,
            extra_user_roles=EXTRA_USER_ROLES,
        )
        self.framework.observe(
            self.first_database.on.database_created, self._on_first_database_created
        )
        self.framework.observe(
            self.first_database.on.endpoints_changed, self._on_first_database_endpoints_changed
        )

        if DATA_INTERFACES_VERSION > 49:
            self.first_database_roles = DatabaseRequires(
                charm=self,
                relation_name="first-database-roles",
                database_name=database_name,
                entity_type=ENTITY_USER,
                extra_user_roles=EXTRA_USER_ROLES,
            )
            self.framework.observe(
                self.first_database_roles.on.database_entity_created,
                self._on_first_database_entity_created,
            )

        # Events related to the second database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f'{self.app.name.replace("-", "_")}_second_database_db'

        # Keeping the charm backwards compatible, for upgrades testing
        if DATA_INTERFACES_VERSION > 17:
            self.second_database = DatabaseRequires(
                charm=self,
                relation_name="second-database-db",
                database_name=database_name,
                extra_user_roles=EXTRA_USER_ROLES,
                additional_secret_fields=["topsecret", "donttellanyone"],
                external_node_connectivity=True,
            )
        else:
            self.second_database = DatabaseRequires(
                charm=self,
                relation_name="second-database-db",
                database_name=database_name,
                extra_user_roles=EXTRA_USER_ROLES,
            )

        self.framework.observe(
            self.second_database.on.database_created, self._on_second_database_created
        )
        self.framework.observe(
            self.second_database.on.endpoints_changed, self._on_second_database_endpoints_changed
        )

        # Multiple database clusters charm events (clusters/relations without alias).
        database_name = f'{self.app.name.replace("-", "_")}_multiple_database_clusters'
        self.database_clusters = DatabaseRequires(
            charm=self,
            relation_name="multiple-database-clusters",
            database_name=database_name,
            extra_user_roles=EXTRA_USER_ROLES,
        )
        self.framework.observe(
            self.database_clusters.on.database_created, self._on_cluster_database_created
        )
        self.framework.observe(
            self.database_clusters.on.endpoints_changed,
            self._on_cluster_endpoints_changed,
        )

        # Multiple database clusters charm events (defined dynamically
        # in the database requires charm library, using the provided cluster/relation aliases).
        database_name = f'{self.app.name.replace("-", "_")}_aliased_multiple_database_clusters'
        cluster_aliases = ["cluster1", "cluster2"]  # Aliases for the multiple clusters/relations.
        self.aliased_database_clusters = DatabaseRequires(
            charm=self,
            relation_name="aliased-multiple-database-clusters",
            database_name=database_name,
            extra_user_roles=EXTRA_USER_ROLES,
            relations_aliases=cluster_aliases,
        )
        # Each database cluster will have its own events
        # with the name having the cluster/relation alias as the prefix.
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_database_created,
            self._on_cluster1_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_endpoints_changed,
            self._on_cluster1_endpoints_changed,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_database_created,
            self._on_cluster2_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_endpoints_changed,
            self._on_cluster2_endpoints_changed,
        )

        # Kafka events

        self.kafka = KafkaRequires(
            charm=self,
            relation_name="kafka-client-topic",
            topic="test-topic",
            extra_user_roles=EXTRA_USER_ROLES_KAFKA,
            consumer_group_prefix=CONSUMER_GROUP_PREFIX,
        )

        if DATA_INTERFACES_VERSION > 34:
            self.kafka_requirer_interface = KafkaRequirerData(
                model=self.model,
                relation_name="kafka-split-pattern-client",
                topic="test-topic-split-pattern",
                extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                consumer_group_prefix=CONSUMER_GROUP_PREFIX,
            )
            self.kafka_split_pattern = KafkaRequirerEventHandlers(
                self, relation_data=self.kafka_requirer_interface
            )
            self.framework.observe(
                self.kafka_split_pattern.on.bootstrap_server_changed,
                self._on_kafka_bootstrap_server_changed,
            )
            self.framework.observe(
                self.kafka_split_pattern.on.topic_created, self._on_kafka_topic_created
            )

        if DATA_INTERFACES_VERSION > 49:
            self.kafka_roles = KafkaRequires(
                charm=self,
                relation_name="kafka-client-roles",
                topic="test-topic",
                entity_type=ENTITY_USER,
                extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                consumer_group_prefix=CONSUMER_GROUP_PREFIX,
            )
            self.framework.observe(
                self.kafka_roles.on.topic_entity_created,
                self._on_kafka_entity_created,
            )

        self.framework.observe(
            self.kafka.on.bootstrap_server_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(self.kafka.on.topic_created, self._on_kafka_topic_created)

        # Kafka Connect events

        if DATA_INTERFACES_VERSION > 52:
            self.connect_source = KafkaConnectRequires(
                self, "connect-source", "http://10.10.10.10:8080"
            )

            self.connect_sink = KafkaConnectRequires(self, "connect-sink", BAD_URL)

            self.framework.observe(
                self.connect_source.on.integration_created, self._on_connect_integration_created
            )

            self.framework.observe(
                self.connect_source.on.integration_endpoints_changed,
                self._on_connect_endpoints_changed,
            )

        # OpenSearch events

        self.opensearch = OpenSearchRequires(
            charm=self,
            relation_name="opensearch-client-index",
            index="test-index",
            extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH,
        )
        self.framework.observe(self.opensearch.on.index_created, self._on_opensearch_index_created)
        self.framework.observe(
            self.opensearch.on.authentication_updated, self._on_opensearch_authentication_updated
        )

        if DATA_INTERFACES_VERSION > 49:
            self.opensearch_roles = OpenSearchRequires(
                charm=self,
                relation_name="opensearch-client-roles",
                index="test-index",
                entity_type=ENTITY_USER,
                extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH,
            )
            self.framework.observe(
                self.opensearch_roles.on.index_entity_created,
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
        self._relation_endpoints = [
            self.first_database,
            self.second_database,
            self.database_clusters,
            self.aliased_database_clusters,
            self.kafka,
            self.opensearch,
        ]

    def _get_relation(self, relation_id: int) -> Optional[Tuple[CharmBase, Relation]]:
        """Retrieve a relation by ID, together with the corresponding endpoint object ('Requires')."""
        for source in self._relation_endpoints:
            for relation in source.relations:
                if relation.id == relation_id:
                    return (source, relation)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    # Generic relation actions
    def _on_get_relation_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            value = source.fetch_relation_field(relation.id, event.params["field"])
        else:
            value = relation.data[relation.app].get(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_get_relation_self_side_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            value = source.fetch_my_relation_field(relation.id, event.params["field"])
        else:
            value = relation.data[source.local_app].get(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_set_relation_field(self, event: ActionEvent):
        """Set requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            source.update_relation_data(
                relation.id, {event.params["field"]: event.params["value"]}
            )
        else:
            relation.data[source.local_app][event.params["field"]] = event.params["value"]

    def _on_delete_relation_field(self, event: ActionEvent):
        """Delete requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            source.delete_relation_data(relation.id, [event.params["field"]])
        else:
            del relation.data[source.local_app][event.params["field"]]

    # First database events observers.
    def _on_first_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"first database credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials of the first database")

    def _on_first_database_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"first database endpoints have been changed to: {event.endpoints}")

    if DATA_INTERFACES_VERSION > 49:

        def _on_first_database_entity_created(self, event: DatabaseEntityCreatedEvent) -> None:
            """Event triggered when a database entity was created for this application."""
            # Retrieve the credentials using the charm library.
            logger.info(f"first database entity credentials: {event.entity_name}")
            self.unit.status = ActiveStatus("received entity credentials of the first database")

    # Second database events observers.
    def _on_second_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"second database credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials of the second database")

    def _on_second_database_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"second database endpoints have been changed to: {event.endpoints}")

    # Multiple database clusters events observers.
    def _on_cluster_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"cluster {event.relation.app.name} credentials: {event.username} {event.password}"
        )
        self.unit.status = ActiveStatus(
            f"received database credentials for cluster {event.relation.app.name}"
        )

    def _on_cluster_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(
            f"cluster {event.relation.app.name} endpoints have been changed to: {event.endpoints}"
        )

    # Multiple database clusters events observers (for aliased clusters/relations).
    def _on_cluster1_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster1 credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster1")

    def _on_cluster1_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster1 endpoints have been changed to: {event.endpoints}")

    def _on_cluster2_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster2 credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster2")

    def _on_cluster2_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster2 endpoints have been changed to: {event.endpoints}")

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

    def _on_kafka_bootstrap_server_changed(self, event: BootstrapServerChangedEvent):
        """Event triggered when a bootstrap server was changed for this application."""
        logger.info(
            f"On kafka boostrap-server changed: bootstrap-server: {event.bootstrap_server}"
        )
        self.unit.status = ActiveStatus("kafka_bootstrap_server_changed")

    def _on_kafka_topic_created(self, _: TopicCreatedEvent):
        """Event triggered when a topic was created for this application."""
        logger.info("On kafka topic created")
        self.unit.status = ActiveStatus("kafka_topic_created")

    if DATA_INTERFACES_VERSION > 49:

        def _on_kafka_entity_created(self, _: TopicEntityCreatedEvent) -> None:
            """Event triggered when a topic entity was created for this application."""
            logger.info("On kafka entity created")
            self.unit.status = ActiveStatus("kafka_entity_created")

    if DATA_INTERFACES_VERSION > 52:

        def _on_connect_integration_created(self, _: IntegrationCreatedEvent):
            """Event triggered when Kafka Connect integration credentials are created for this application."""
            self.unit.status = ActiveStatus("connect_integration_created")

        def _on_connect_endpoints_changed(self, _: IntegrationEndpointsChangedEvent):
            """Event triggered when Kafka Connect REST endpoints change."""
            self.unit.status = ActiveStatus("connect_endpoints_changed")

    def _on_opensearch_index_created(self, _: IndexCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch index created event fired")
        self.unit.status = ActiveStatus("opensearch_index_created")

    if DATA_INTERFACES_VERSION > 49:

        def _on_opensearch_entity_created(self, _: IndexEntityCreatedEvent):
            """Event triggered when an index entity was created for this application."""
            logger.info("On opensearch entity created event fired")
            self.unit.status = ActiveStatus("opensearch_entity_created")

    def _on_opensearch_authentication_updated(self, _: IndexCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch authentication_updated event fired")
        self.unit.status = ActiveStatus("opensearch_authentication_updated")

    def _on_reset_unit_status(self, _: ActionEvent):
        """Handle the reset of status message for the unit."""
        self.unit.status = ActiveStatus()

    def _on_set_mtls_cert(self, event: ActionEvent):
        """Sets the MTLS cert for the relation."""
        if DATA_INTERFACES_VERSION < 47:
            event.fail("This action is not supported on lib version < 0.47.")
            return

        cmd = f'openssl req -new -newkey rsa:2048 -days 365 -nodes -subj "/CN={self.unit.name.replace("/", "-")}" -x509 -keyout client.key -out client.pem'
        subprocess.check_output(cmd, shell=True, universal_newlines=True)
        cert = open("./client.pem", "r").read()
        relation = self.model.get_relation("kafka-split-pattern-client")
        self.kafka_requirer_interface.set_mtls_cert(relation.id, cert)
        event.set_results({"mtls-cert": cert})


if __name__ == "__main__":
    main(ApplicationCharm)
