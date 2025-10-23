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
from typing import Annotated, Optional

import psycopg2
from ops import Relation, Unit
from ops.charm import ActionEvent, CharmBase, WorkloadEvent
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus
from pydantic import Field

from charms.data_platform_libs.v1.data_interfaces import (
    DataContractV1,
    ExtraSecretStr,
    OpsOtherPeerUnitRepositoryInterface,
    OpsPeerRepositoryInterface,
    OpsPeerUnitRepositoryInterface,
    OptionalSecretStr,
    PeerModel,
    RequirerCommonModel,
    ResourceEntityRequestedEvent,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    ResourceRequestedEvent,
)

logger = logging.getLogger(__name__)

SECRET_INTERNAL_LABEL = "internal-secret"
PEER = "database-peers"
APP_SECRETS = ["monitor-password", "secret-field"]
UNIT_SECRETS = ["monitor-password", "secret-field", "my-unit-secret"]

MygroupSecretStr = Annotated[OptionalSecretStr, Field(exclude=True, default=None), "mygroup"]


class PeerAppModel(PeerModel):
    monitor_password: ExtraSecretStr
    secret_field: ExtraSecretStr
    mygroup_field1: MygroupSecretStr = Field(default=None)
    mygroup_field2: MygroupSecretStr = Field(default=None)
    not_a_secret: str | None = Field(default=None)


class PeerUnitModel(PeerModel):
    monitor_password: ExtraSecretStr
    secret_field: ExtraSecretStr
    my_unit_secret: ExtraSecretStr
    not_a_secret: str | None = Field(default=None)
    non_secret_field: str | None = Field(default=None)


class ExtendedResourceProviderModel(ResourceProviderModel):
    topsecret: ExtraSecretStr = Field(default=None)
    new_field: str | None = Field(default=None)
    new_field2: str | None = Field(default=None)


DataContract = DataContractV1[ExtendedResourceProviderModel]


class DatabaseCharm(CharmBase):
    """Database charm that accepts connections from application charms."""

    _stored = StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self._servers_data = {}

        self._peer_relation_app = OpsPeerRepositoryInterface(
            self.model, relation_name=PEER, data_model=PeerAppModel
        )
        self._peer_relation_unit = OpsPeerUnitRepositoryInterface(
            self.model, relation_name=PEER, data_model=PeerUnitModel
        )

        # Default charm events.
        self.framework.observe(self.on.database_pebble_ready, self._on_database_pebble_ready)

        # Charm events defined in the database provides charm library.
        # self.database = DatabaseProvides(self, relation_name="database")
        self.database = ResourceProviderEventHandler(self, "database", RequirerCommonModel)
        self.framework.observe(self.database.on.resource_requested, self._on_resource_requested)
        self.framework.observe(
            self.database.on.resource_entity_requested, self._on_resource_entity_requested
        )

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
    def peer_units_data_interfaces(self) -> dict[Unit, OpsOtherPeerUnitRepositoryInterface]:
        """The cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        for unit in self.peer_relation.units:
            if unit not in self._servers_data:
                self._servers_data[unit] = OpsOtherPeerUnitRepositoryInterface(
                    model=self.model, relation_name=PEER, unit=unit, data_model=PeerUnitModel
                )
        return self._servers_data

    def _on_change_admin_password(self, event: ActionEvent):
        """Change the admin password."""
        password = self._new_password()
        for relation in self.database.interface.relations:
            model = self.database.interface.build_model(relation.id, DataContract)
            for request in model.requests:
                request.password = password  # pyright: ignore[reportAttributeAccessIssue]
            self.database.interface.write_model(relation.id, model)

    def _on_set_secret_action(self, event: ActionEvent):
        """Change the admin password."""
        secret_field: str | None = event.params.get("field")
        rel_id = event.params.get("relation_id")
        if not secret_field or not rel_id:
            event.fail("Invalid empty field.")
            return
        password = event.params.get("value", self._new_password())
        for relation in self.database.interface.relations:
            if relation.id == rel_id:
                break
        model = self.database.interface.build_model(relation.id, DataContract)
        for request in model.requests:
            setattr(request, secret_field, password)
        self.database.interface.write_model(relation.id, model)

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

    def _on_resource_requested(self, event: ResourceRequestedEvent) -> None:
        """Event triggered when a new database is requested."""
        self.unit.status = MaintenanceStatus("creating database")

        relation_id = event.relation.id

        request = event.request

        resource = request.resource
        extra_user_roles = request.extra_user_roles

        username = f"relation_{relation_id}_{request.request_id}"
        password = self._new_password()
        connection_string = (
            "dbname='postgres' user='postgres' host='localhost' "
            f"password='{self._stored.password}' connect_timeout=10"
        )
        connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        cursor = connection.cursor()
        # Create the database, user and password. Also gives the user access to the database.
        cursor.execute(f"CREATE DATABASE {resource};")
        cursor.execute(f"CREATE USER {username} WITH ENCRYPTED PASSWORD '{password}';")
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {resource} TO {username};")
        # Add the roles to the user.
        if extra_user_roles:
            cursor.execute(f"ALTER USER {username} {extra_user_roles};")
        # Get the database version.
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        cursor.close()
        connection.close()

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

        response = ResourceProviderModel(
            request_id=request.request_id,
            resource=resource,
            password=password,
            username=username,
            endpoints=f"{self.model.get_binding('database').network.bind_address}:5432",
            tls=False,
            version=version,
        )
        self.database.set_response(event.relation.id, response)
        self.unit.status = ActiveStatus()

    def _on_resource_entity_requested(self, event: ResourceEntityRequestedEvent) -> None:
        """Event triggered when a new database entity is requested."""
        self.unit.status = MaintenanceStatus("creating entity")

        request = event.request

        # Retrieve the entity-type using the charm library.
        entity_type = request.entity_type

        # Generate a entity-name and a entity-password for the application.
        rolename = self._new_rolename()
        password = self._new_password()

        # Connect to the database.
        connection_string = (
            "dbname='postgres' user='postgres' host='localhost' "
            f"password='{self._stored.password}' connect_timeout=10"
        )
        connection = psycopg2.connect(connection_string)
        connection.autocommit = True
        cursor = connection.cursor()

        # Create the role
        if entity_type == "user":
            extra_roles = request.extra_user_roles
            cursor.execute(f"CREATE ROLE {rolename} WITH ENCRYPTED PASSWORD '{password}';")
            cursor.execute(f"ALTER ROLE {rolename} {extra_roles};")
        if entity_type == "group":
            extra_roles = request.extra_group_roles
            cursor.execute(f"CREATE ROLE {rolename};")
            cursor.execute(f"ALTER ROLE {rolename} {extra_roles};")

        # Share the credentials with the application.
        response = ResourceProviderModel(
            request_id=request.request_id,
            salt=request.salt,
            entity_name=rolename,
            entity_password=password,
        )
        self.database.set_response(event.relation.id, response)
        self.unit.status = ActiveStatus()

    def _get_relation(self, relation_id: int) -> Relation:
        for relation in self.database.interface.relations:
            if relation.id == relation_id:
                return relation
        raise ValueError(f"No relation {relation_id}")

    # Get/set/delete field on second-database relation
    def _on_get_relation_field(self, event: ActionEvent):
        """[second_database]: Get requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        value = None
        model = self.database.interface.build_model(
            relation.id, DataContract, component=relation.app
        )
        for request in model.requests:
            value = getattr(request, event.params["field"].replace("-", "_"))
        event.set_results({"value": value if value else ""})

    def _on_get_relation_self_side_field(self, event: ActionEvent):
        """[second_database]: Get requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        value = None
        model = self.database.interface.build_model(relation.id, DataContract)
        for request in model.requests:
            value = getattr(request, event.params["field"].replace("-", "_"))
        event.set_results({"value": value if value else ""})

    def _on_set_relation_field(self, event: ActionEvent):
        """Set requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        model = self.database.interface.build_model(relation.id, DataContract)
        for request in model.requests:
            setattr(request, event.params["field"].replace("-", "_"), event.params["value"])
        self.database.interface.write_model(relation.id, model)

    def _on_delete_relation_field(self, event: ActionEvent):
        """Delete requested relation field."""
        relation = self._get_relation(event.params["relation_id"])
        model = self.database.interface.build_model(relation.id, DataContract)
        for request in model.requests:
            setattr(request, event.params["field"].replace("-", "_"), None)
        # Charms should be compatible with old vesrions, to simulatrams["field"])
        self.database.interface.write_model(relation.id, model)

    def _new_rolename(self) -> str:
        """Generate a random rolename string.

        Returns:
           A random rolename string.
        """
        choices = string.ascii_letters
        return "".join([secrets.choice(choices) for _ in range(8)])

    def _new_password(self) -> str:
        """Generate a random password string.

        Returns:
           A random password string.
        """
        choices = string.ascii_letters + string.digits
        return "".join([secrets.choice(choices) for _ in range(16)])

    # Get/set/delete field on the peer relation
    def _on_get_peer_relation_field(self, event: ActionEvent):
        """[second_database]: Set requested relation field."""
        component = event.params["component"]

        value = None

        if component == "app":
            relation = self._peer_relation_app.relations[0]
            model = self._peer_relation_app.build_model(relation.id)
            value = getattr(model, event.params["field"].replace("-", "_"))
        else:
            relation = self._peer_relation_unit.relations[0]
            model = self._peer_relation_unit.build_model(relation.id)
            value = getattr(model, event.params["field"].replace("-", "_"))
        event.set_results({"value": value if value else ""})

    def _on_set_peer_relation_field(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]
        if component == "app":
            relation = self._peer_relation_app.relations[0]
            model = self._peer_relation_app.build_model(relation.id)
            setattr(model, event.params["field"].replace("-", "_"), event.params["value"])
            self._peer_relation_app.write_model(relation.id, model)
        else:
            relation = self._peer_relation_unit.relations[0]
            model = self._peer_relation_unit.build_model(relation.id)
            setattr(model, event.params["field"].replace("-", "_"), event.params["value"])
            self._peer_relation_unit.write_model(relation.id, model)

    def _on_set_peer_relation_field_multiple(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]
        count = event.params["count"]

        # Charms should be compatible with old vesrions, to simulate rolling upgrade
        for cnt in range(count):
            value = event.params["value"] + f"{cnt}"
            if component == "app":
                relation = self._peer_relation_app.relations[0]
                repository = self._peer_relation_app.repository(relation.id)
                repository.write_field(event.params["field"], value)
            else:
                relation = self._peer_relation_unit.relations[0]
                repository = self._peer_relation_unit.repository(relation.id)
                repository.write_field(event.params["field"], value)

    def _on_set_peer_secret(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]
        if component == "app":
            relation = self._peer_relation_app.relations[0]
            repository = self._peer_relation_app.repository(relation.id)
            repository.write_secret_field(
                event.params["field"], event.params["value"], secret_group=event.params["group"]
            )
        else:
            relation = self._peer_relation_unit.relations[0]
            repository = self._peer_relation_unit.repository(relation.id)
            repository.write_secret_field(
                event.params["field"], event.params["value"], secret_group=event.params["group"]
            )

    def _on_delete_peer_relation_field(self, event: ActionEvent):
        """Delete requested relation field."""
        component = event.params["component"]

        if component == "app":
            relation = self._peer_relation_app.relations[0]
            model = self._peer_relation_app.build_model(relation.id)
            setattr(model, event.params["field"].replace("-", "_"), None)
            self._peer_relation_app.write_model(relation.id, model)
        else:
            relation = self._peer_relation_unit.relations[0]
            model = self._peer_relation_unit.build_model(relation.id)
            setattr(model, event.params["field"].replace("-", "_"), None)
            self._peer_relation_unit.write_model(relation.id, model)

    # Other Peer Data
    def _on_get_other_peer_relation_field(self, event: ActionEvent):
        """[second_database]: Get requested relation field."""
        value = {}
        relation = self.model.get_relation(PEER)
        if not relation:
            event.fail("Missing relation")
            return
        for unit, interface in self.peer_units_data_interfaces.items():
            model = interface.build_model(relation.id)
            value[unit.name.replace("/", "-")] = getattr(
                model, event.params["field"].replace("-", "_")
            )
        for key, item in value.items():
            value[key] = item
        event.set_results(value)

    # Remove peer secrets
    def _on_delete_peer_secret(self, event: ActionEvent):
        """Delete requested relation field."""
        component = event.params["component"]

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
