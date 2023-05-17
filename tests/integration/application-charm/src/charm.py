#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

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
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)

# Extra roles that this application needs when interacting with the database.
EXTRA_USER_ROLES = "SUPERUSER"
EXTRA_USER_ROLES_KAFKA = "producer,consumer"
EXTRA_USER_ROLES_OPENSEARCH = "admin,default"
CONSUMER_GROUP_PREFIX = "test-prefix"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.get_plugin_status_action, self._on_get_plugin_status)

        # Events related to the first database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f'{self.app.name.replace("-", "_")}_first_database'
        self.first_database = DatabaseRequires(
            self, "first-database", database_name, EXTRA_USER_ROLES
        )
        self.framework.observe(
            self.first_database.on.database_created, self._on_first_database_created
        )
        self.framework.observe(
            self.first_database.on.endpoints_changed, self._on_first_database_endpoints_changed
        )

        # Events related to the second database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f'{self.app.name.replace("-", "_")}_second_database'
        self.second_database = DatabaseRequires(
            self, "second-database", database_name, EXTRA_USER_ROLES
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
            self, "multiple-database-clusters", database_name, EXTRA_USER_ROLES
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
            self,
            "aliased-multiple-database-clusters",
            database_name,
            EXTRA_USER_ROLES,
            cluster_aliases,
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
            self, "kafka-client", "test-topic", EXTRA_USER_ROLES_KAFKA, CONSUMER_GROUP_PREFIX
        )

        self.framework.observe(
            self.kafka.on.bootstrap_server_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(self.kafka.on.topic_created, self._on_kafka_topic_created)

        # OpenSearch events

        self.opensearch = OpenSearchRequires(
            self, "opensearch-client", "test-index", EXTRA_USER_ROLES_OPENSEARCH
        )
        self.framework.observe(self.opensearch.on.index_created, self._on_opensearch_index_created)

        # actions

        self.framework.observe(self.on.reset_unit_status_action, self._on_reset_unit_status)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    # First database events observers.
    def _on_first_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"first database credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials of the first database")

    def _on_first_database_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"first database endpoints have been changed to: {event.endpoints}")

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

    def _on_opensearch_index_created(self, _: IndexCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch index created event fired")
        self.unit.status = ActiveStatus("opensearch_index_created")

    def _on_reset_unit_status(self, _: ActionEvent):
        """Handle the reset of status message for the unit."""
        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(ApplicationCharm)
