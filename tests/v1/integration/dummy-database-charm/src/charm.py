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
from typing import Annotated, Optional

from ops import Relation
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus
from pydantic import Field

from charms.data_platform_libs.v1.data_interfaces import (
    DataContractV1,
    ExtraSecretStr,
    OpsOtherPeerUnitRepository,
    OpsPeerRepositoryInterface,
    OpsPeerUnitRepositoryInterface,
    OptionalSecretStr,
    PeerModel,
    RequirerCommonModel,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    SecretGroup,
)

logger = logging.getLogger(__name__)

PEER = "database-peers"


MygroupSecretStr = Annotated[OptionalSecretStr, Field(exclude=True, default=None), "mygroup"]


class PeerAppModel(PeerModel):
    field: MygroupSecretStr = Field(default=None)
    not_a_secret: str | None = Field(default=None)
    new_field: ExtraSecretStr = Field(default=None)
    mygroup_field1: MygroupSecretStr = Field(default=None)
    mygroup_field2: MygroupSecretStr = Field(default=None)


class ExtendedResourceProviderModel(ResourceProviderModel):
    field: MygroupSecretStr = Field(default=None)
    not_a_secret: str | None = Field(default=None)
    new_field: ExtraSecretStr = Field(default=None)


ExtendedDataContractV1 = DataContractV1[ExtendedResourceProviderModel]


class DatabaseCharm(CharmBase):
    """Database charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)
        self._servers_data = {}

        self.peer_relation_app = OpsPeerRepositoryInterface(
            self.model, relation_name=PEER, data_model=PeerAppModel
        )

        self.peer_relation_unit = OpsPeerUnitRepositoryInterface(
            self.model, relation_name=PEER, data_model=PeerAppModel
        )

        self.database = ResourceProviderEventHandler(self, "database", RequirerCommonModel)

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
    def peer_relation(self) -> Optional[Relation]:
        """The cluster peer relation."""
        return self.model.get_relation(PEER)

    @property
    def peer_units_data_interfaces(self) -> dict:
        """The cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        for unit in self.peer_relation.units:
            if unit not in self._servers_data:
                self._servers_data[unit] = OpsOtherPeerUnitRepository(
                    self.model,
                    relation=self.peer_relation,
                    component=unit,
                )
        return self._servers_data

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _on_change_admin_password(self, event: ActionEvent):
        """Change the admin password."""
        password = self._new_password()
        for relation in self.database.interface.relations:
            model: DataContractV1 = self.database.interface.build_model(
                relation.id, ExtendedDataContractV1
            )
            for request in model.requests:
                request.password = password
            self.database.interface.write_model(relation.id, model)

    def _on_set_secret_action(self, event: ActionEvent):
        """Change the admin password."""
        if not (password := event.params["password"]):
            password = self._new_password()
        for relation in self.database.interface.relations:
            model: DataContractV1 = self.database.interface.build_model(
                relation.id, ExtendedDataContractV1
            )
            field: str = event.params.get("field") or "value"
            if not ExtendedResourceProviderModel._get_secret_field(field):
                event.fail("Invalid secret field.")
            for request in model.requests:
                setattr(request, field, password)
            self.database.interface.write_model(relation.id, model)

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
            relation_bis = self.peer_relation_app.relations[0]
            repository = self.peer_relation_app.repository(relation_bis.id)
            result = repository.get_secret_field(event.params["field"], event.params["group"])

        else:
            relation_bis = self.peer_relation_unit.relations[0]
            repository = self.peer_relation_unit.repository(relation_bis.id)
            result = repository.get_secret_field(event.params["field"], event.params["group"])

        event.set_results({event.params["field"]: result if result else ""})

    def _on_set_peer_secret(self, event: ActionEvent):
        """Set requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old versions, to simulate rolling upgrade
        if component == "app":
            relation_bis = self.peer_relation_app.relations[0]
            repository = self.peer_relation_app.repository(relation_bis.id)
            repository.write_secret_field(
                event.params["field"],
                event.params["value"],
                event.params["group"] or SecretGroup("extra"),
            )
        else:
            relation_bis = self.peer_relation_unit.relations[0]
            repository = self.peer_relation_unit.repository(relation_bis.id)
            repository.write_secret_field(
                event.params["field"],
                event.params["value"],
                event.params["group"] or SecretGroup("extra"),
            )

    # Remove peer secrets
    def _on_delete_peer_secret(self, event: ActionEvent):
        """Delete requested relation field."""
        component = event.params["component"]

        # Charms should be compatible with old versions, to simulate rolling upgrade
        secret = None
        group_str = "" if not event.params["group"] else f".{event.params['group']}"
        if component == "app":
            secret = self.model.get_secret(label=f"{PEER}.{self.app.name}.app{group_str}")
        else:
            secret = self.model.get_secret(label=f"{PEER}.{self.app.name}.unit{group_str}")

        if secret:
            secret.remove_all_revisions()

    # Get/set/delete field on the peer relation
    def _on_get_peer_relation_field(self, event: ActionEvent):
        """[second_database]: Set requested relation field."""
        component = event.params["component"]

        value = None
        if component == "app":
            relation = self.peer_relation_app.relations[0]
            model = self.peer_relation_app.build_model(relation.id)
            value = getattr(model, event.params["field"].replace("-", "_"))
        else:
            relation = self.peer_relation_unit.relations[0]
            model = self.peer_relation_unit.build_model(relation.id)
            value = getattr(model, event.params["field"].replace("-", "_"))
        event.set_results({"value": value if value else ""})


if __name__ == "__main__":
    main(DatabaseCharm)
