#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import secrets
import string

import psycopg2
from charms.data_platform_libs.v0.database_provides import DatabaseProvides
from ops.charm import CharmBase, WorkloadEvent
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)


class DatabaseCharm(CharmBase):
    """Database charm that accepts connections from application charms."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.database_pebble_ready, self._on_database_pebble_ready)

        # Charm events defined in the database provides charm library.
        self.database = DatabaseProvides(self, relation_name="database")
        self.framework.observe(self.database.on.database_requested, self._on_database_requested)

        # Stored state is used to track the password of the database superuser.
        self._stored.set_default(password=self._new_password())

    def _on_database_pebble_ready(self, event: WorkloadEvent) -> None:
        """Define and start the database using the Pebble API."""
        container = event.workload
        pebble_layer = {
            "summary": "database layer",
            "description": "pebble config layer for database",
            "services": {
                "database": {
                    "override": "replace",
                    "summary": "database",
                    "command": "/usr/local/bin/docker-entrypoint.sh postgres",
                    "startup": "enabled",
                    "environment": {
                        "PGDATA": "/var/lib/postgresql/data/pgdata",
                        "POSTGRES_PASSWORD": self._stored.password,
                    },
                }
            },
        }
        container.add_layer("database", pebble_layer, combine=True)
        container.autostart()
        self.unit.status = ActiveStatus()

    def _on_database_requested(self, _) -> None:
        """Event triggered when a new database is requested."""
        self.unit.status = MaintenanceStatus("creating database")

        # Retrieve the database name and extra user roles using the charm library.
        database = self.database.database
        extra_user_roles = self.database.extra_user_roles

        # Generate a username and a password for the application.
        username = f"juju_{database}"
        password = self._new_password()

        # Connect to the database.
        connection_string = f"dbname='postgres' user='postgres' host='localhost' password='{self._stored.password}' connect_timeout=10"
        connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        # Create the database, user and password. Also gives the user access to the database.
        cursor.execute(f"CREATE DATABASE {database};")
        cursor.execute(f"CREATE USER {username} WITH ENCRYPTED PASSWORD '{password}';")
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {database} TO {username};")
        # Add the roles to the user.
        if extra_user_roles:
            cursor.execute(f'ALTER USER {username} {extra_user_roles.replace(",", " ")};')
        # Get the database version.
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        cursor.close()
        connection.close()

        # Share the credentials with the application.
        self.database.set_credentials(username, password)

        # Set the read/write endpoint.
        self.database.set_endpoints(
            f'{self.model.get_binding("database").network.bind_address}:5432'
        )

        # Share additional information with the application.
        self.database.set_tls("False")
        self.database.set_version(version)

        self.unit.status = ActiveStatus()

    def _new_password(self) -> str:
        """Generate a random password string.

        Returns:
           A random password string.
        """
        choices = string.ascii_letters + string.digits
        return "".join([secrets.choice(choices) for i in range(16)])


if __name__ == "__main__":
    main(DatabaseCharm)
