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
from random import randrange
from time import sleep

import psycopg2
from ops import Relation
from ops.charm import ActionEvent, CharmBase, WorkloadEvent
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

from charms.data_platform_libs.v0.data_interfaces import (
    LIBPATCH as DATA_INTERFACES_VERSION,
)
from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseProvides,
    DatabaseRequestedEvent,
)

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
        self.framework.observe(
            self.on.change_admin_password_action, self._on_change_admin_password
        )

        self.framework.observe(self.on.set_secret_action, self._on_set_secret_action)

        # Get/set/delete values on second-database relaton
        self.framework.observe(
            self.on.get_relation_self_side_field_action, self._on_get_relation_self_side_field
        )
        self.framework.observe(self.on.get_relation_field_action, self._on_get_relation_field)
        self.framework.observe(self.on.set_relation_field_action, self._on_set_relation_field)
        self.framework.observe(
            self.on.delete_relation_field_action, self._on_delete_relation_field
        )

    def _on_change_admin_password(self, event: ActionEvent):
        """Change the admin password."""
        password = self._new_password()
        for relation in self.database.relations:
            self.database.update_relation_data(relation.id, {"password": password})

    def _on_set_secret_action(self, event: ActionEvent):
        """Change the admin password."""
        secret_field = event.params.get("field")
        password = self._new_password()
        for relation in self.database.relations:
            self.database.update_relation_data(relation.id, {secret_field: password})

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

    def _on_database_requested(self, event: DatabaseRequestedEvent) -> None:
        """Event triggered when a new database is requested."""
        self.unit.status = MaintenanceStatus("creating database")

        # Retrieve the database name and extra user roles using the charm library.
        database = event.database
        extra_user_roles = event.extra_user_roles

        # Generate a username and a password for the application.
        username = f"juju_{database}"
        password = self._new_password()

        # Connect to the database.
        connection_string = (
            "dbname='postgres' user='postgres' host='localhost' "
            f"password='{self._stored.password}' connect_timeout=10"
        )
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
        self.database.set_credentials(event.relation.id, username, password)

        # Temporary hack to avoid https://bugs.launchpad.net/juju/+bug/2031631
        sleep(randrange(5))

        assert self.model.get_binding("database")
        assert self.model.get_binding("database").network
        assert self.model.get_binding("database").network.bind_address
        logger.info(
            (
                f"Charm binding {self.model.get_binding('database')}, "
                f"network: {self.model.get_binding('database').network}, "
                f"IP: {self.model.get_binding('database').network.bind_address}"
            )
        )

        # Set the read/write endpoint.
        self.database.set_endpoints(
            event.relation.id, f'{self.model.get_binding("database").network.bind_address}:5432'
        )

        # Share additional information with the application.
        self.database.set_tls(event.relation.id, "False")
        self.database.set_version(event.relation.id, version)

        self.unit.status = ActiveStatus()

    def _get_relation(self, relation_id: int) -> Relation:
        for relation in self.database.relations:
            if relation.id == relation_id:
                return relation

    # Get/set/delete field on second-database relation
    def _on_get_relation_field(self, event: ActionEvent):
        """[second_database]: Set requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        value = None
        if DATA_INTERFACES_VERSION > 17:
            value = self.database.fetch_relation_field(relation.id, event.params["field"])
        else:
            value = relation.data[relation.app].get(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_get_relation_self_side_field(self, event: ActionEvent):
        """[second_database]: Set requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        value = None
        if DATA_INTERFACES_VERSION > 17:
            value = self.database.fetch_my_relation_field(relation.id, event.params["field"])
        else:
            value = relation.data[self.database.local_app].get(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_set_relation_field(self, event: ActionEvent):
        """Set requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        if DATA_INTERFACES_VERSION > 17:
            logger.info(
                "*************************************** Setting (secret?) field ***************************************"
            )
            self.database.update_relation_data(
                relation.id, {event.params["field"]: event.params["value"]}
            )
        else:
            relation.data[self.database.local_app].update(
                {event.params["field"]: event.params["value"]}
            )

    def _on_delete_relation_field(self, event: ActionEvent):
        """Delete requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        if DATA_INTERFACES_VERSION > 17:
            self.database.delete_relation_data(relation.id, [event.params["field"]])
        else:
            relation.data[self.database.local_app].pop(event.params["field"])

    def _new_password(self) -> str:
        """Generate a random password string.

        Returns:
           A random password string.
        """
        choices = string.ascii_letters + string.digits
        return "".join([secrets.choice(choices) for i in range(16)])


if __name__ == "__main__":
    main(DatabaseCharm)
