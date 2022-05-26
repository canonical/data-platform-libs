#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms."""

import logging

from charms.data_platform_libs.v0.database_requires import DatabaseRequires
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)

# Name of the database that this application requests to the database charm.
DATABASE = "data_platform"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.database_relation_joined, self._on_database_relation_joined)
        self.framework.observe(self.on.get_database_action, self._on_get_database)
        self.framework.observe(self.on.get_username_action, self._on_get_username)
        self.framework.observe(self.on.get_password_action, self._on_get_password)
        self.framework.observe(self.on.get_version_action, self._on_get_version)
        self.framework.observe(
            self.on.request_extra_user_roles_action, self._on_request_extra_user_roles
        )
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the database requires charm library.
        self.database = DatabaseRequires(self, "database")
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_endpoints_changed)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _on_database_relation_joined(self, _) -> None:
        """Requests the creation of a new database to the database charm."""
        self.unit.status = MaintenanceStatus("awaiting database creation")
        self.database.set_database(DATABASE)

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

    def _on_get_database(self, event: ActionEvent) -> None:
        """Returns the name of the database that was created for this application."""
        event.set_results({"database": DATABASE})

    def _on_get_password(self, event: ActionEvent) -> None:
        """Returns the password of the user that was created for this application."""
        event.set_results({"password": self.database.password})

    def _on_get_username(self, event: ActionEvent) -> None:
        """Returns the user that was created for this application."""
        event.set_results({"username": self.database.username})

    def _on_get_version(self, event: ActionEvent) -> None:
        """Returns the version of the database that this application is connected to."""
        event.set_results({"version": self.database.version})

    def _on_request_extra_user_roles(self, event: ActionEvent) -> None:
        """Request extra user roles from the database."""
        roles = event.params["roles"]
        self.database.set_extra_user_roles(roles)


if __name__ == "__main__":
    main(ApplicationCharm)
