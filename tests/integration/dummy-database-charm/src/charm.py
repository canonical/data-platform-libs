#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Database charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import secrets
import string

from ops import Relation
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus

from charms.data_platform_libs.v0.data_interfaces import (
    DataPeer,
    DataPeerOtherUnit,
    DataPeerUnit,
)

logger = logging.getLogger(__name__)

PEER = "database-peers"


class DatabaseCharm(CharmBase):
    """Database charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)
        self._servers_data = {}

        self.peer_relation_app = DataPeer(
            self,
            relation_name=PEER,
            additional_secret_group_mapping={"mygroup": []},
        )
        self.peer_relation_unit = DataPeerUnit(self, relation_name=PEER)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Stored state is used to track the password of the database superuser.
        self.framework.observe(
            self.on.change_admin_password_action, self._on_change_admin_password
        )

        self.framework.observe(self.on.set_secret_action, self._on_set_secret_action)

        # Get/set/delete values on database relaton
        self.framework.observe(
            self.on.get_peer_relation_field_action, self._on_get_peer_relation_field
        )

        self.framework.observe(self.on.set_peer_secret_action, self._on_set_peer_secret)
        self.framework.observe(self.on.get_peer_secret_action, self._on_get_peer_secret)
        self.framework.observe(self.on.delete_peer_secret_action, self._on_delete_peer_secret)

    @property
    def peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(PEER)

    @property
    def peer_units_data_interfaces(self) -> dict:
        """The cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        for unit in self.peer_relation.units:
            if unit not in self._servers_data:
                self._servers_data[unit] = DataPeerOtherUnit(
                    charm=self,
                    unit=unit,
                    relation_name=PEER,
                )
        return self._servers_data

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _on_change_admin_password(self, event: ActionEvent):
        """Change the admin password."""
        password = self._new_password()
        for relation in self.database.relations:
            self.database.set_secret(relation.id, {"password": password})

    def _on_set_secret_action(self, event: ActionEvent):
        """Change the admin password."""
        if not (password := event.param["password"]):
            password = self._new_password()
        for relation in self.database.relations:
            self.database.set_secret(
                relation.id, event.params.get("field"), password, event.params.get("group")
            )

    def _get_relation(self, relation_id: int) -> Relation:
        for relation in self.database.relations:
            if relation.id == relation_id:
                return relation

    def _new_password(self) -> str:
        """Generate a random password string.

        Returns:
           A random password string.
        """
        choices = string.ascii_letters + string.digits
        return "".join([secrets.choice(choices) for i in range(16)])

    def _on_get_peer_secret(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old versions, to simulate rolling upgrade
        if component == "app":
            relation = self.peer_relation_app.relations[0]
            self.peer_relation_app.get_secret(
                relation.id,
                event.params["field"],
                event.params["value"],
                event.params["group"],
            )
        else:
            relation = self.peer_relation_unit.relations[0]
            self.peer_relation_unit.get_secret(
                relation.id,
                event.params["field"],
                event.params["value"],
                event.params["group"],
            )

    def _on_set_peer_secret(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old versions, to simulate rolling upgrade
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

    # Remove peer secrets
    def _on_delete_peer_secret(self, event: ActionEvent):
        """Delete requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old versions, to simulate rolling upgrade
        secret = None
        group_str = "" if not event.params["group"] else f".{event.params['group']}"
        if component == "app":
            secret = self.model.get_secret(label=f"{self.app.name}.app{group_str}")
        else:
            secret = self.model.get_secret(label=f"{self.app.name}.unit{group_str}")

        if secret:
            secret.remove_all_revisions()

    # Get/set/delete field on the peer relation
    def _on_get_peer_relation_field(self, event: ActionEvent):
        """[second_database]: Set requested relation field."""
        component = event.params["component"]

        value = None
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


if __name__ == "__main__":
    main(DatabaseCharm)
