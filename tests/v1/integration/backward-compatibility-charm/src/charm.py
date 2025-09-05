#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Client charm that creates connection to database charm.

This charm is meant to be used only for testing of the libraries in this repository.
This uses the v0 of data interfaces to ensure that the compatibility is not broken.
"""

import logging

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus

from charms.data_platform_libs.v0.data_interfaces import DatabaseCreatedEvent, DatabaseRequires

logger = logging.getLogger(__name__)


class ClientCharm(CharmBase):
    """Database charm that accepts connections from application charms."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        # Charm events defined in the database provides charm library.
        self.database = DatabaseRequires(self, "backward-database", "bwclient")
        self.framework.observe(self.database.on.database_created, self._on_resource_created)

    def _on_resource_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a new database is requested."""
        relation_id = event.relation.id
        username = event.username
        password = event.password
        database = event.database

        logger.error(
            f"Database {database} created for relation {relation_id} with user {username} and password {password}"
        )
        self.unit.status = ActiveStatus("backward_database_created")


if __name__ == "__main__":
    main(ClientCharm)
