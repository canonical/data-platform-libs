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
from typing import Optional

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

if DATA_INTERFACES_VERSION > 17:
    from charms.data_platform_libs.v0.data_interfaces import (
        DataPeer,
        DataPeerOtherUnit,
        DataPeerUnit,
    )


logger = logging.getLogger(__name__)

SECRET_INTERNAL_LABEL = "internal-secret"
SECRET_DELETED_LABEL = "#DELETED#"
PEER = "database-peers"
APP_SECRETS = ["monitor-password", "secret-field"]
UNIT_SECRETS = ["monitor-password", "secret-field", "my-unit-secret"]


class DatabaseCharm(CharmBase):
    """Database charm that accepts connections from application charms."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self._servers_data = {}

        if DATA_INTERFACES_VERSION > 17:
            self.peer_relation_app = DataPeer(
                self,
                relation_name=PEER,
                additional_secret_fields=APP_SECRETS,
                additional_secret_group_mapping={"mygroup": ["mygroup-field1", "mygroup-field2"]},
                secret_field_name=SECRET_INTERNAL_LABEL,
                deleted_label=SECRET_DELETED_LABEL,
            )
            self.peer_relation_unit = DataPeerUnit(
                self,
                relation_name=PEER,
                additional_secret_fields=UNIT_SECRETS,
                secret_field_name=SECRET_INTERNAL_LABEL,
                deleted_label=SECRET_DELETED_LABEL,
            )

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

        # Get/set/delete values on second-database relaton
        self.framework.observe(
            self.on.get_peer_relation_field_action, self._on_get_peer_relation_field
        )
        self.framework.observe(
            self.on.set_peer_relation_field_action, self._on_set_peer_relation_field
        )
        self.framework.observe(
            self.on.set_peer_relation_field_multiple_action,
            self._on_set_peer_relation_field_multiple,
        )
        self.framework.observe(self.on.set_peer_secret_action, self._on_set_peer_secret)
        self.framework.observe(
            self.on.delete_peer_relation_field_action, self._on_delete_peer_relation_field
        )
        self.framework.observe(self.on.delete_peer_secret_action, self._on_delete_peer_secret)

        self.framework.observe(
            self.on.get_other_peer_relation_field_action, self._on_get_other_peer_relation_field
        )

    @property
    def peer_relation(self) -> Optional[Relation]:
        """The cluster peer relation."""
        return self.model.get_relation(PEER)

    @property
    def peer_units_data_interfaces(self) -> dict:
        """The cluster peer relation."""
        if DATA_INTERFACES_VERSION <= 17:
            return {}

        if not self.peer_relation or not self.peer_relation.units:
            return {}

        for unit in self.peer_relation.units:
            if unit not in self._servers_data:
                self._servers_data[unit] = DataPeerOtherUnit(
                    charm=self,
                    unit=unit,
                    relation_name=PEER,
                    additional_secret_fields=UNIT_SECRETS,
                )
        return self._servers_data

    def _on_change_admin_password(self, event: ActionEvent):
        """Change the admin password."""
        password = self._new_password()
        for relation in self.database.relations:
            self.database.update_relation_data(relation.id, {"password": password})

    def _on_set_secret_action(self, event: ActionEvent):
        """Change the admin password."""
        secret_field = event.params.get("field")
        password = event.params.get("value", self._new_password())
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

    # Get/set/delete field on the peer relation
    def _on_get_peer_relation_field(self, event: ActionEvent):
        """[second_database]: Set requested relation field."""
        component = event.params["component"]

        value = None
        if DATA_INTERFACES_VERSION <= 17:
            relation = self.model.get_relation(PEER)
            if component == "app":
                value = relation.data[self.app][event.params["field"]]
            else:
                value = relation.data[self.unit][event.params["field"]]
        else:
            if component == "app":
                relation = self.peer_relation_app.relations[0]
                value = self.peer_relation_app.fetch_my_relation_field(
                    relation.id, event.params["field"]
                )
            else:
                relation = self.peer_relation_unit.relations[0]
                value = self.peer_relation_unit.fetch_my_relation_field(
                    relation.id, event.params["field"]
                )
        event.set_results({"value": value if value else ""})

    def _on_set_peer_relation_field(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        if DATA_INTERFACES_VERSION <= 17:
            relation = self.model.get_relation(PEER)
            if component == "app":
                relation.data[self.app][event.params["field"]] = event.params["value"]
            else:
                relation.data[self.unit][event.params["field"]] = event.params["value"]
            return

        if component == "app":
            relation = self.peer_relation_app.relations[0]
            self.peer_relation_app.update_relation_data(
                relation.id, {event.params["field"]: event.params["value"]}
            )
        else:
            relation = self.peer_relation_unit.relations[0]
            self.peer_relation_unit.update_relation_data(
                relation.id, {event.params["field"]: event.params["value"]}
            )

    def _on_set_peer_relation_field_multiple(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]
        count = event.params["count"]

        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        for cnt in range(count):
            value = event.params["value"] + f"{cnt}"
            if DATA_INTERFACES_VERSION <= 17:
                relation = self.model.get_relation(PEER)
                if component == "app":
                    relation.data[self.app][event.params["field"]] = value
                else:
                    relation.data[self.unit][event.params["field"]] = value
                return

            if component == "app":
                relation = self.peer_relation_app.relations[0]
                self.peer_relation_app.update_relation_data(
                    relation.id, {event.params["field"]: value}
                )
            else:
                relation = self.peer_relation_unit.relations[0]
                self.peer_relation_unit.update_relation_data(
                    relation.id, {event.params["field"]: value}
                )

    def _on_set_peer_secret(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        if DATA_INTERFACES_VERSION <= 17:
            relation = self.model.get_relation(PEER)
            if component == "app":
                relation.data[self.app][event.params["field"]] = event.params["value"]
            else:
                relation.data[self.unit][event.params["field"]] = event.params["value"]
            return

        if component == "app":
            relation = self.peer_relation_app.relations[0]
            self.peer_relation_app.set_secret(
                relation.id,
                event.params["field"],
                event.params["value"],
                event.params["group"],
            )
        else:
            relation = self.peer_relation_unit.relations[0]
            self.peer_relation_unit.set_secret(
                relation.id,
                event.params["field"],
                event.params["value"],
                event.params["group"],
            )

    def _on_delete_peer_relation_field(self, event: ActionEvent):
        """Delete requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        if DATA_INTERFACES_VERSION <= 17:
            relation = self.model.get_relation(PEER)
            if component == "app":
                del relation.data[self.app][event.params["field"]]
            else:
                del relation.data[self.unit][event.params["field"]]
            return

        if component == "app":
            relation = self.peer_relation_app.relations[0]
            self.peer_relation_app.delete_relation_data(relation.id, [event.params["field"]])
        else:
            relation = self.peer_relation_unit.relations[0]
            self.peer_relation_unit.delete_relation_data(relation.id, [event.params["field"]])

    # Other Peer Data
    def _on_get_other_peer_relation_field(self, event: ActionEvent):
        """[second_database]: Get requested relation field."""
        value = {}
        relation = self.model.get_relation(PEER)
        if DATA_INTERFACES_VERSION > 17:
            for unit, interface in self.peer_units_data_interfaces.items():
                value[unit.name.replace("/", "-")] = interface.fetch_my_relation_field(
                    relation.id, event.params["field"]
                )
        event.set_results(value)

    # Remove peer secrets
    def _on_delete_peer_secret(self, event: ActionEvent):
        """Delete requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        if DATA_INTERFACES_VERSION <= 17:
            return

        secret = None
        group_str = "" if not event.params["group"] else f".{event.params['group']}"
        if component == "app":
            secret = self.model.get_secret(label=f"{PEER}.database.app{group_str}")
        else:
            secret = self.model.get_secret(label=f"{PEER}.database.unit{group_str}")

        if secret:
            secret.remove_all_revisions()


if __name__ == "__main__":
    main(DatabaseCharm)
