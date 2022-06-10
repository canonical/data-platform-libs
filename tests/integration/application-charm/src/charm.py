#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from charms.data_platform_libs.v0.database_requires import (
    DatabaseCreatedEvent,
    DatabaseEndpointsChangedEvent,
    DatabaseRequires,
)
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)

# Extra roles that this application needs when interacting with the database.
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Name of the database that this application requests to the database charm.
        database = f'{self.app.name.replace("-", "_")}_database'
        # Charm events defined in the database requires charm library.
        self.database = DatabaseRequires(self, "database", database, EXTRA_USER_ROLES)
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_endpoints_changed)

        # Multiple database clusters charm events (defined dynamically
        # in the database requires charm library, using the provided cluster/relation aliases).
        database_name = f'{self.app.name.replace("-", "_")}_multiple_database_clusters'
        cluster_aliases = ["cluster1", "cluster2"]  # Aliases for the multiple clusters/relations.
        self.database_clusters = DatabaseRequires(
            self, "multiple-database-clusters", database_name, EXTRA_USER_ROLES, cluster_aliases
        )
        # Each database cluster will have its own events
        # with the name having the cluster/relation alias as the prefix.
        self.framework.observe(
            self.database_clusters.on.cluster1_database_created, self._on_cluster1_database_created
        )
        self.framework.observe(
            self.database_clusters.on.cluster1_endpoints_changed,
            self._on_cluster1_endpoints_changed,
        )
        self.framework.observe(
            self.database_clusters.on.cluster2_database_created, self._on_cluster2_database_created
        )
        self.framework.observe(
            self.database_clusters.on.cluster2_endpoints_changed,
            self._on_cluster2_endpoints_changed,
        )

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    # First database events.
    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"_on_database_created called: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials")

    def _on_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"_on_endpoints_changed called: {event.endpoints}")

    # Multiple database clusters events observers.
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


if __name__ == "__main__":
    main(ApplicationCharm)
