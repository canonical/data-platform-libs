#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from charms.data_platform_libs.v0.database_requires import DatabaseRequires
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus

logger = logging.getLogger(__name__)

# Name of the database that this application requests to the database charm.
DATABASE = "data_platform"
# Extra roles that this application needs when interacting with the database.
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the database requires charm library.
        self.database = DatabaseRequires(self, "database", DATABASE, EXTRA_USER_ROLES)
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_endpoints_changed)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _on_database_created(self, _) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"_on_database_created called: {self.database.username} {self.database.password}"
        )
        self.unit.status = ActiveStatus("received database credentials")

    def _on_endpoints_changed(self, _) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"_on_endpoints_changed called: {self.database.endpoints}")


if __name__ == "__main__":
    main(ApplicationCharm)
