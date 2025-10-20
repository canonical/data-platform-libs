# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import re
import unittest
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Annotated, Dict, Tuple, Type
from unittest.mock import Mock, patch

import pytest
from ops.charm import CharmBase
from ops.model import Relation, Unit
from ops.testing import Harness
from parameterized import parameterized
from pydantic import Field, SecretStr, TypeAdapter, ValidationError

from charms.data_platform_libs.v0.data_interfaces import (
    PROV_SECRET_PREFIX,
)
from charms.data_platform_libs.v1.data_interfaces import (
    Diff,
    EntityPermissionModel,
    ExtraSecretStr,
    KafkaRequestModel,
    OpsOtherPeerUnitRepository,
    OpsPeerRepositoryInterface,
    OpsPeerUnitRepositoryInterface,
    OpsRepository,
    OptionalSecretStr,
    PeerModel,
    RepositoryInterface,
    RequirerCommonModel,
    ResourceCreatedEvent,
    ResourceEndpointsChangedEvent,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    ResourceReadOnlyEndpointsChangedEvent,
    ResourceRequestedEvent,
    ResourceRequirerEventHandler,
    ResourceRequiresEvents,
    SecretGroup,
    TCommon,
    TRepository,
)
from charms.harness_extensions.v0.capture_events import capture, capture_events

logger = getLogger(__name__)

ENTITY_GROUP = "GROUP"
ENTITY_USER = "USER"

PEER_RELATION_NAME = "database-peers"

DATABASE = "data_platform"
ENTITY_PERMISSIONS = [
    {"resource_name": "cars", "resource_type": "TABLE", "privileges": ["SELECT"]}
]
EXTRA_USER_ROLES: str = "CREATEDB,CREATEROLE"
EXTRA_GROUP_ROLES: str = "CUSTOM_ROLE_1,CUSTOM_ROLE_2"
DATABASE_RELATION_INTERFACE = "database_client"
DATABASE_RELATION_NAME = "database"
DATABASE_METADATA = f"""
name: database

peers:
  database-peers:
    interface: database-peers

provides:
  {DATABASE_RELATION_NAME}:
    interface: {DATABASE_RELATION_INTERFACE}
"""


CLUSTER_ALIASES = ["cluster1", "cluster2"]
METADATA = f"""
name: application
requires:
  {DATABASE_RELATION_NAME}:
    interface: {DATABASE_RELATION_INTERFACE}
    limit: {len(CLUSTER_ALIASES)}
"""

#
# Helper functions
#


def verify_relation_interface_functions(
    interface: RepositoryInterface[TRepository, TCommon], relation_id: int
):
    """This function is used to verify that the 3 main interface functions work correctly."""
    repository = interface.repository(relation_id)
    for field in ["something", "secret-field"]:
        # Interface function: write_field
        repository.write_field(field=field, value="else")

        # Interface function: get_field
        assert repository.get_field(field) == "else"

        # Interface function: get_fields
        assert repository.get_fields(field) == {field: "else"}

        # Interface function: delete_field
        repository.delete_field(field)

        assert repository.get_field(field) is None
        rel_data = repository.get_fields(field)
        assert rel_data == {}


#
# Test CharmsOptionalSecretStr
#

MygroupSecretStr = Annotated[OptionalSecretStr, Field(exclude=True, default=None), "mygroup"]


class PeerAppModel(PeerModel):
    secret_field_app: ExtraSecretStr = Field(alias="secret-field-app")
    secret_field: ExtraSecretStr = Field(alias="secret-field")
    mysecret1: MygroupSecretStr
    mysecret2: MygroupSecretStr


class PeerUnitModel(PeerModel):
    secret_field_unit: ExtraSecretStr = Field(alias="secret-field-unit")
    secret_field: ExtraSecretStr = Field(alias="secret-field")
    mysecret1: MygroupSecretStr
    mysecret2: MygroupSecretStr


class DatabaseCharm(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.peer_relation_app = OpsPeerRepositoryInterface(
            self.model, PEER_RELATION_NAME, data_model=PeerAppModel
        )
        self.peer_relation_unit = OpsPeerUnitRepositoryInterface(
            self.model, PEER_RELATION_NAME, data_model=PeerUnitModel
        )
        self.provider = ResourceProviderEventHandler(
            self, DATABASE_RELATION_NAME, RequirerCommonModel
        )
        self._servers_data = {}
        self.framework.observe(
            self.provider.on.resource_requested,
            self._on_resource_requested,
        )
        self.framework.observe(
            self.provider.on.resource_entity_requested,
            self._on_resource_entity_requested,
        )
        self.framework.observe(
            self.provider.on.resource_entity_permissions_changed,
            self._on_resource_entity_permissions_changed,
        )

    @property
    def peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(PEER_RELATION_NAME)

    @property
    def peer_units_data_interfaces(self) -> Dict[Unit, OpsOtherPeerUnitRepository]:
        """The cluster peer relation."""
        if not self.peer_relation or not self.peer_relation.units:
            return {}

        for unit in self.peer_relation.units:
            if unit not in self._servers_data:
                self._servers_data[unit] = OpsOtherPeerUnitRepository(
                    self.model, relation=self.peer_relation, component=unit
                )
        return self._servers_data

    def _on_resource_requested(self, _) -> None:
        pass

    def _on_resource_entity_requested(self, _) -> None:
        pass

    def _on_resource_entity_permissions_changed(self, _) -> None:
        pass


class DatabaseCharmDynamicSecrets(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.peer_relation_app = OpsPeerRepositoryInterface(
            self.model, PEER_RELATION_NAME, data_model=PeerAppModel
        )
        self.peer_relation_unit = OpsPeerUnitRepositoryInterface(
            self.model, PEER_RELATION_NAME, data_model=PeerUnitModel
        )

    @property
    def peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(PEER_RELATION_NAME)


#
# Tests
#


class DataProvidesBaseTests(ABC):
    SECRET_FIELDS = [
        "username",
        "password",
        "tls",
        "tls-ca",
        "uris",
        "read-only-uris",
        "entity-name",
        "entity-password",
    ]

    DATABASE_FIELD = "resource"

    app_name: str
    relation_name: str

    @pytest.fixture
    def use_caplog(self, caplog):
        self._caplog = caplog

    @abstractmethod
    def get_harness(self) -> Tuple[Harness, int]:
        pass

    def setUp(self):
        self.harness, self.rel_id = self.get_harness()

    def tearDown(self) -> None:
        self.harness.cleanup()

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        relation = self.harness.model.get_relation(self.relation_name, self.rel_id)
        # Use a variable to easily update the relation changed event data during the test.

        data_model = ResourceProviderModel.model_validate(
            {"resource": "blah", "request-id": "", "secret-user": "secret://deabeef"}
        )
        # Test with new data added to the relation databag.
        result = self.harness.charm.provider.compute_diff(relation, request=data_model, store=True)
        assert result == Diff({"request-id", "salt", "resource", "secret-user"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.provider.compute_diff(relation, request=data_model, store=True)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data_model.resource = "bluh"
        result = self.harness.charm.provider.compute_diff(relation, request=data_model, store=True)
        assert result == Diff(set(), {"resource"}, set())

        # Test with deleted data.
        del data_model.secret_user
        result = self.harness.charm.provider.compute_diff(relation, request=data_model, store=True)
        assert result == Diff(set(), set(), {"secret-user"})

    def test_relation_interface(self):
        """Check the functionality of each public interface function."""
        # We pretend that the connection is initialized
        self.harness.update_relation_data(
            self.rel_id, "application", {self.DATABASE_FIELD: DATABASE}
        )

        interface = self.harness.charm.provider.interface
        verify_relation_interface_functions(interface, self.rel_id)

    def test_set_credentials_secrets(self):
        """Asserts that credentials are set up as secrets if possible."""
        # Set some data in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            self.DATABASE_FIELD: DATABASE,
                            "request-id": "c759221a6c14c72a",
                            "salt": "kkkkkkkk",
                        }
                    ]
                ),
            },
        )
        response = ResourceProviderModel(
            salt="kkkkkkkk",
            request_id="c759221a6c14c72a",
            resource=DATABASE,
            username=SecretStr("test-username"),
            password=SecretStr("test-password"),
        )

        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.provider.set_response(self.rel_id, response)

        # Check that the credentials are present in the relation.
        relation = self.harness.get_relation_data(self.rel_id, self.app_name)
        assert json.loads(relation["data"]) == (
            {
                "c759221a6c14c72a": {
                    self.DATABASE_FIELD: DATABASE,
                    "salt": "kkkkkkkk",
                    "request-id": "c759221a6c14c72a",
                }
            }
        )

        requests = json.loads(relation["requests"])
        secret_id = requests[0][f"{PROV_SECRET_PREFIX}user"]
        secret = self.harness.charm.model.get_secret(id=secret_id)

        assert secret.get_content(refresh=True) == {
            "username": "test-username",
            "password": "test-password",
        }

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_entity_credentials(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        # Set some data in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            self.DATABASE_FIELD: DATABASE,
                            "request-id": "c759221a6c14c72a",
                            "salt": "kkkkkkkk",
                            "entity-type": "USER",
                        }
                    ]
                ),
            },
        )

        # Set the entity credentials in the relation using the provides charm library.
        response = ResourceProviderModel(
            salt="kkkkkkkk",
            request_id="c759221a6c14c72a",
            resource=DATABASE,
            entity_name=SecretStr("test-name"),  # pyright: ignore[reportCallIssue]
            entity_password=SecretStr("test-password"),  # pyright: ignore[reportCallIssue]
        )

        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.provider.set_response(self.rel_id, response)

        relation = self.harness.get_relation_data(self.rel_id, self.app_name)

        # Check that the entity credentials are present in the relation.
        assert relation["data"] == json.dumps(
            {
                "c759221a6c14c72a": {
                    self.DATABASE_FIELD: DATABASE,
                    "request-id": "c759221a6c14c72a",
                    "salt": "kkkkkkkk",
                    "entity-type": "USER",
                }
            }
        )

        requests = json.loads(relation["requests"])
        secret_id = requests[0][f"{PROV_SECRET_PREFIX}entity"]
        secret = self.harness.charm.model.get_secret(id=secret_id)

        assert secret.get_content(refresh=True) == {
            "entity-name": "test-name",
            "entity-password": "test-password",
        }


class TestDatabaseProvides(DataProvidesBaseTests, unittest.TestCase):
    metadata = DATABASE_METADATA
    relation_name = DATABASE_RELATION_NAME
    app_name = "database"
    charm = DatabaseCharm

    def get_harness(self) -> Tuple[Harness, int]:
        harness = Harness(self.charm, meta=self.metadata)
        # Set up the initial relation and hooks.
        rel_id = harness.add_relation(self.relation_name, "application")
        peer_rel_id = harness.add_relation(PEER_RELATION_NAME, self.app_name)
        harness.add_relation_unit(peer_rel_id, f"{self.app_name}/1")
        harness.add_relation_unit(peer_rel_id, f"{self.app_name}/2")

        harness.add_relation_unit(rel_id, "application/0")
        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    #
    # Peer Data tests
    #
    def test_other_peer_relation_disabled_functions(self):
        """Verify that fetch_relation_data/field() functions are disabled for Peer Relations."""
        for _, repository in self.harness.charm.peer_units_data_interfaces.items():
            with pytest.raises(NotImplementedError):
                repository.write_field("key", "value")

            with pytest.raises(NotImplementedError):
                repository.delete_field("key")

    @parameterized.expand([("peer_relation_app",), ("peer_relation_unit",)])
    def test_peer_relation_interface(self, interface_attr):
        """Check the functionality of each public interface function."""
        interface = getattr(self.harness.charm, interface_attr)
        verify_relation_interface_functions(interface, self.harness.charm.peer_relation.id)

    @parameterized.expand([("peer_relation_app",), ("peer_relation_unit",)])
    def test_peer_relation_interface_secret_fields(self, interface_attr):
        """Check the functionality of each public interface function."""
        relation_id: int = self.harness.charm.peer_relation.id
        interface: RepositoryInterface = getattr(self.harness.charm, interface_attr)

        model = interface.build_model(relation_id)

        model.secret_field = "bla"
        model.mysecret1 = "bla"

        interface.write_model(relation_id, model)

        repository = interface.repository(relation_id)
        assert repository.get_secret_field("secret-field", "extra") == "bla"
        assert repository.get_secret_field("mysecret1", "mygroup") == "bla"

    @parameterized.expand([("peer_relation_app",), ("peer_relation_unit",)])
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_peer_relation_secret_secret_revision(self, interface_attr):
        """Check the functionality of each public interface function."""
        # Given
        relation_id: int = self.harness.charm.peer_relation.id
        interface: RepositoryInterface = getattr(self.harness.charm, interface_attr)
        repository = interface.repository(relation_id)

        scope = interface_attr.split("_")[2]
        scope_opj = getattr(self.harness.charm, scope)
        secret = scope_opj.add_secret(
            {"secret-field": "initialvalue"}, label=f"{PEER_RELATION_NAME}.database.{scope}"
        )
        cached_secret = repository.secrets.get(label=f"{PEER_RELATION_NAME}.database.{scope}")

        initial_secret_revision = secret.get_info().revision
        initial_cached_secret_revision = cached_secret.meta.get_info().revision

        # When
        repository.write_secret_field("secret-field", "initialvalue", "extra")
        secret.get_content(refresh=True)

        unchanged_secret_revision = secret.get_info().revision
        unchanged_cached_secret_revision = cached_secret.meta.get_info().revision

        repository.write_secret_field("secret-field", "newvalue", "extra")
        secret.get_content(refresh=True)

        changed_secret_revision = secret.get_info().revision
        changed_cached_secret_revision = cached_secret.meta.get_info().revision

        # Then
        assert (
            initial_secret_revision
            == initial_cached_secret_revision
            == unchanged_secret_revision
            == unchanged_cached_secret_revision
        )
        assert changed_secret_revision == unchanged_secret_revision + 1
        assert changed_cached_secret_revision == unchanged_cached_secret_revision + 1

    def test_peer_relation_other_unit(self):
        """Check the functionality of each public interface function on each "other" unit."""
        relation_id = self.harness.charm.peer_relation.id
        for unit, interface in self.harness.charm.peer_units_data_interfaces.items():
            interface: OpsOtherPeerUnitRepository
            self.harness.update_relation_data(relation_id, unit.name, {"something": "else"})

            # fetch_relation_field()
            assert interface.get_field("something") == "else"

            # fetch_relation_data()
            rel_data = interface.get_fields("something")
            assert rel_data["something"] == "else"

            assert interface.get_field("non-existent-field") is None
            rel_data = interface.get_fields("non-existent-field")
            assert rel_data == {}

    def test_peer_relation_other_unit_dict(self):
        """Check the functionality of each public interface function on each "other" unit."""
        relation_id = self.harness.charm.peer_relation.id
        for unit, interface in self.harness.charm.peer_units_data_interfaces.items():
            interface: OpsOtherPeerUnitRepository
            self.harness.update_relation_data(relation_id, unit.name, {"something": "else"})

            # fetch_relation_field()
            assert interface.get_field("something") == "else"

            # fetch_relation_data()
            rel_data = interface.get_data()
            assert rel_data
            assert rel_data["something"] == "else"

            with pytest.raises(KeyError):
                assert rel_data["non-existent-field"]
            assert rel_data.get("non-existent-field") is None

    #
    # Relation Data tests
    #
    @patch.object(DatabaseCharm, "_on_resource_requested")
    def test_on_resource_requested_v0(self, _on_resource_requested):
        """Asserts that the correct hook is called when a new database is requested."""
        # Simulate the request of a new database plus extra user roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                self.DATABASE_FIELD: DATABASE,
                "extra-user-roles": EXTRA_USER_ROLES,
                "external-node-connectivity": "true",
            },
        )

        # Assert the correct hook is called.
        _on_resource_requested.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_requested.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.extra_user_roles == EXTRA_USER_ROLES
        assert event.request.external_node_connectivity is True

        # Reset the mock call count.
        _on_resource_requested.reset_mock()

        # Simulate the request of a new database entity.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {self.DATABASE_FIELD: DATABASE, "entity-type": ENTITY_USER},
        )

        # Assert the correct hook is called.
        _on_resource_requested.assert_not_called()

    @patch.object(DatabaseCharm, "_on_resource_requested")
    def test_on_resource_requested_v1(self, _on_resource_requested):
        """Asserts that the correct hook is called when a new database is requested."""
        # Simulate the request of a new database plus extra user roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            self.DATABASE_FIELD: DATABASE,
                            "extra-user-roles": EXTRA_USER_ROLES,
                            "external-node-connectivity": True,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                        }
                    ]
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_requested.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_requested.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.extra_user_roles == EXTRA_USER_ROLES
        assert event.request.external_node_connectivity is True

        # Reset the mock call count.
        _on_resource_requested.reset_mock()

        # Simulate the request of a new database entity.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            self.DATABASE_FIELD: DATABASE,
                            "entity-type": ENTITY_USER,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                        }
                    ]
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_requested.assert_not_called()

    @patch.object(DatabaseCharm, "_on_resource_entity_requested")
    def test_on_resource_entity_requested_v0(self, _on_resource_entity_requested):
        """Asserts that the correct hook is called when a new database entity is requested."""
        # Simulate the request of a new user plus extra roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                self.DATABASE_FIELD: DATABASE,
                "entity-type": ENTITY_USER,
                "entity-permissions": json.dumps(ENTITY_PERMISSIONS),
                "extra-user-roles": json.dumps(EXTRA_USER_ROLES),
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_requested.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_entity_requested.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.entity_type == ENTITY_USER
        assert [
            item.model_dump() for item in event.request.entity_permissions
        ] == ENTITY_PERMISSIONS
        assert event.request.extra_user_roles == EXTRA_USER_ROLES

        # Reset the relation data keys + mock count
        self.harness.update_relation_data(self.rel_id, self.app_name, {"data": "{}"})
        _on_resource_entity_requested.reset_mock()

        # Simulate the request of a new group plus extra roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                self.DATABASE_FIELD: DATABASE,
                "entity-type": ENTITY_GROUP,
                "entity-permissions": json.dumps(ENTITY_PERMISSIONS),
                "extra-user-roles": "",
                "extra-group-roles": EXTRA_GROUP_ROLES,
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_requested.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_entity_requested.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.entity_type == ENTITY_GROUP
        assert [
            item.model_dump() for item in event.request.entity_permissions
        ] == ENTITY_PERMISSIONS
        assert event.request.extra_group_roles == EXTRA_GROUP_ROLES

    @patch.object(DatabaseCharm, "_on_resource_entity_requested")
    def test_on_resource_entity_requested_v1(self, _on_resource_entity_requested):
        """Asserts that the correct hook is called when a new database entity is requested."""
        # Simulate the request of a new user plus extra roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            self.DATABASE_FIELD: DATABASE,
                            "entity-type": ENTITY_USER,
                            "entity-permissions": ENTITY_PERMISSIONS,
                            "extra-user-roles": EXTRA_USER_ROLES,
                        }
                    ]
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_requested.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_entity_requested.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.entity_type == ENTITY_USER
        assert [
            item.model_dump() for item in event.request.entity_permissions
        ] == ENTITY_PERMISSIONS
        assert event.request.extra_user_roles == EXTRA_USER_ROLES

        # Reset the relation data keys + mock count
        self.harness.update_relation_data(self.rel_id, self.app_name, {"data": "{}"})
        _on_resource_entity_requested.reset_mock()

        # Simulate the request of a new group plus extra roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            self.DATABASE_FIELD: DATABASE,
                            "entity-type": ENTITY_GROUP,
                            "entity-permissions": ENTITY_PERMISSIONS,
                            "extra-user-roles": "",
                            "extra-group-roles": EXTRA_GROUP_ROLES,
                        }
                    ]
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_requested.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_entity_requested.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.entity_type == ENTITY_GROUP
        assert [
            item.model_dump() for item in event.request.entity_permissions
        ] == ENTITY_PERMISSIONS
        assert event.request.extra_group_roles == EXTRA_GROUP_ROLES

    @patch.object(DatabaseCharm, "_on_resource_entity_permissions_changed")
    def test_on_entity_permissions_changed_v0(self, _on_resource_entity_permissions_changed):
        """Asserts that the correct hook is called when entity permissions are changed."""
        # Simulate the request of a new user plus extra roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                self.DATABASE_FIELD: DATABASE,
                "entity-type": ENTITY_USER,
                "entity-permissions": "",
                "extra-user-roles": EXTRA_USER_ROLES,
            },
        )

        # Simulate the request to update user permissions.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                self.DATABASE_FIELD: DATABASE,
                "entity-type": ENTITY_USER,
                "entity-permissions": json.dumps(ENTITY_PERMISSIONS),
                "extra-user-roles": EXTRA_USER_ROLES,
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_permissions_changed.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_entity_permissions_changed.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.entity_type == ENTITY_USER
        assert event.request.entity_permissions == TypeAdapter(
            list[EntityPermissionModel]
        ).validate_python(ENTITY_PERMISSIONS)
        assert event.request.extra_user_roles == EXTRA_USER_ROLES

    @patch.object(DatabaseCharm, "_on_resource_entity_permissions_changed")
    def test_on_entity_permissions_changed_v1(self, _on_resource_entity_permissions_changed):
        """Asserts that the correct hook is called when entity permissions are changed."""
        # Simulate the request of a new user plus extra roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            self.DATABASE_FIELD: DATABASE,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "entity-type": ENTITY_USER,
                            "extra-user-roles": EXTRA_USER_ROLES,
                        }
                    ]
                ),
            },
        )

        # Simulate the request to update user permissions.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            self.DATABASE_FIELD: DATABASE,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "entity-type": ENTITY_USER,
                            "entity-permissions": ENTITY_PERMISSIONS,
                            "extra-user-roles": EXTRA_USER_ROLES,
                        }
                    ]
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_permissions_changed.assert_called_once()

        # Assert the database name and the entity info are accessible in the providers charm library event.
        event = _on_resource_entity_permissions_changed.call_args[0][0]
        assert event.request.resource == DATABASE
        assert event.request.entity_type == ENTITY_USER
        assert event.request.entity_permissions == TypeAdapter(
            list[EntityPermissionModel]
        ).validate_python(ENTITY_PERMISSIONS)
        assert event.request.extra_user_roles == EXTRA_USER_ROLES

    def test_database_requested_event(self):
        # Test custom event creation

        # Test the event being emitted by the application.
        with capture(self.harness.charm, ResourceRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})
        assert captured.event.app.name == "application"

        # Reset the diff data to trigger the event again later.
        self.harness.update_relation_data(self.rel_id, "database", {"data": "{}"})

        # Test the event being emitted by the unit.
        with capture(self.harness.charm, ResourceRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application/0", {"database": DATABASE})
        assert captured.event.unit.name == "application/0"


class TestDatabaseProvidesDynamicSecrets(ABC, unittest.TestCase):
    metadata = DATABASE_METADATA
    relation_name = DATABASE_RELATION_NAME
    app_name = "database"
    charm = DatabaseCharmDynamicSecrets

    def get_harness(self) -> Harness:
        harness = Harness(self.charm, meta=self.metadata)
        # Set up the initial relation and hooks.
        peer_rel_id = harness.add_relation(PEER_RELATION_NAME, self.app_name)
        harness.add_relation_unit(peer_rel_id, f"{self.app_name}/1")
        harness.add_relation_unit(peer_rel_id, f"{self.app_name}/2")
        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness

    def setUp(self):
        self.harness = self.get_harness()

    def tearDown(self) -> None:
        self.harness.cleanup()

    #
    # Peer Data tests
    #
    @parameterized.expand([("app",), ("unit",)])
    def test_peer_relation_interface(self, scope):
        """Check the functionality of each public interface function."""
        interface: OpsPeerRepositoryInterface = getattr(
            self.harness.charm, f"peer_relation_{scope}"
        )
        relation_id = self.harness.charm.peer_relation.id

        repository = interface.repository(relation_id)

        # set_secret()
        repository.write_secret_field("something", "else", SecretGroup("extra"))

        secret = self.harness.charm.model.get_secret(
            label=f"{PEER_RELATION_NAME}.database.{scope}"
        )
        assert "something" in secret.get_content()

        # get_secret()
        assert repository.get_secret_field("something", SecretGroup("extra")) == "else"

        # delete_secret()
        repository.delete_secret_field("something", SecretGroup("extra"))

        assert repository.get_secret_field("something", SecretGroup("extra")) is None


class ApplicationCharmDatabase(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = ResourceRequirerEventHandler(
            self,
            relation_name=DATABASE_RELATION_NAME,
            requests=[
                RequirerCommonModel(
                    resource=DATABASE, extra_user_roles=EXTRA_USER_ROLES, salt="kkkkkkkk"
                ),
                RequirerCommonModel(resource="", entity_type="USER", salt="xxxxxxxx"),
            ],
            relation_aliases=CLUSTER_ALIASES,
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.requirer.on.resource_created,
            self._on_resource_created,
        )
        self.framework.observe(
            self.requirer.on.resource_entity_created,
            self._on_resource_entity_created,
        )
        self.framework.observe(
            self.on[DATABASE_RELATION_NAME].relation_broken, self._on_relation_broken
        )
        self.framework.observe(self.requirer.on.endpoints_changed, self._on_endpoints_changed)
        self.framework.observe(
            self.requirer.on.read_only_endpoints_changed,
            self._on_read_only_endpoints_changed,
        )
        self.framework.observe(
            self.requirer.on.cluster1_resource_created,
            self._on_cluster1_resource_created,
        )

    def log_relation_size(self, prefix=""):
        logger.info(f"§{prefix} relations: {len(self.requirer.interface.relations)}")

    @staticmethod
    def get_relation_size(log_message: str) -> int:
        num_of_relations = (
            re.search(r"relations: [0-9]*", log_message)
            .group(0)
            .replace("relations: ", "")
            .strip()
        )

        return int(num_of_relations)

    @staticmethod
    def get_prefix(log_message: str) -> str:
        return (
            re.search(r"§.* relations:", log_message)
            .group(0)
            .replace("relations:", "")
            .replace("§", "")
            .strip()
        )

    def _on_resource_created(self, _) -> None:
        self.log_relation_size("on_resource_created")

    def _on_resource_entity_created(self, _) -> None:
        self.log_relation_size("on_resource_entity_created")

    def _on_relation_broken(self, event) -> None:
        # This should not raise errors
        self.requirer.interface.repository(event.relation.id).get_data()

        self.log_relation_size("on_relation_broken")

    def _on_endpoints_changed(self, _) -> None:
        self.log_relation_size("on_endpoints_changed")

    def _on_read_only_endpoints_changed(self, _) -> None:
        self.log_relation_size("on_read_only_endpoints_changed")

    def _on_cluster1_resource_created(self, _) -> None:
        self.log_relation_size("on_cluster1_resource_created")


@pytest.fixture(autouse=True)
def reset_aliases():
    """Fixture that runs before each test to delete the custom events created for the aliases.

    This is needed because the events are created again in the next test,
    which causes an error related to duplicated events.
    """
    for cluster_alias in CLUSTER_ALIASES:
        try:
            delattr(ResourceRequiresEvents, f"{cluster_alias}_resource_created")
            delattr(ResourceRequiresEvents, f"{cluster_alias}_resource_entity_created")
            delattr(ResourceRequiresEvents, f"{cluster_alias}_endpoints_changed")
            delattr(ResourceRequiresEvents, f"{cluster_alias}_read_only_endpoints_changed")
        except AttributeError:
            # Ignore the events not existing before the first test.
            pass


class DataRequirerBaseTests(ABC):
    metadata: str
    relation_name: str
    app_name: str
    charm: Type[CharmBase]

    rel_id: int

    @pytest.fixture
    def use_caplog(self, caplog):
        self._caplog = caplog

    def get_harness(self) -> Harness:
        harness = Harness(self.charm, meta=self.metadata)
        harness.set_leader(True)
        return harness

    def add_relation(self, harness: Harness, app_name: str) -> int:
        rel_id = harness.add_relation(self.relation_name, app_name)
        harness.add_relation_unit(rel_id, f"{app_name}/0")
        return rel_id

    def setUp(self):
        self.harness = self.get_harness()
        self.harness.begin_with_initial_hooks()

    def tearDown(self) -> None:
        self.harness.cleanup()

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        application = "data-platform"

        rel_id = self.add_relation(self.harness, application)
        relation = self.harness.model.get_relation(self.relation_name, rel_id)

        data_model = ResourceProviderModel.model_validate(
            {"resource": "blah", "request-id": "", "secret-user": "secret://deadbeef"}
        )

        # Test with new data added to the relation databag.
        result = self.harness.charm.requirer.compute_diff(relation, request=data_model, store=True)
        assert result == Diff({"request-id", "salt", "resource", "secret-user"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.requirer.compute_diff(relation, request=data_model, store=True)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data_model.resource = "bluh"
        result = self.harness.charm.requirer.compute_diff(relation, request=data_model, store=True)
        assert result == Diff(set(), {"resource"}, set())

        # Test with deleted data.
        del data_model.secret_user
        result = self.harness.charm.requirer.compute_diff(relation, request=data_model, store=True)
        assert result == Diff(set(), set(), {"secret-user"})

    def test_relation_interface(self):
        """Check the functionality of each public interface function."""
        interface = self.harness.charm.requirer.interface
        verify_relation_interface_functions(interface, self.rel_id)

    def test_relation_interface_consistency(self):
        """Check the consistency of the public interface init function."""
        with pytest.raises(ValueError):
            RequirerCommonModel(entity_type="INVALID_ROLE_TYPE")
        with pytest.raises(ValueError):
            RequirerCommonModel(entity_type="USER", extra_group_roles=EXTRA_GROUP_ROLES)
        with pytest.raises(ValueError):
            RequirerCommonModel(entity_type="GROUP", extra_user_roles=EXTRA_USER_ROLES)
        with pytest.raises(ValidationError):
            KafkaRequestModel(consumer_group_prefix="*")


class TestDatabaseRequiresNoRelations(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = DATABASE_RELATION_NAME
    charm = ApplicationCharmDatabase

    app_name = "application"
    provider = "database"

    def setUp(self):
        self.harness = self.get_harness()
        self.harness.begin_with_initial_hooks()

    def test_relation_interface(self):
        """Disabling irrelevant inherited test."""
        pass

    def test_relation_interface_dict(self):
        """Disabling irrelevant inherited test."""
        pass

    def test_hide_relation_on_broken_event(self):
        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )
        with self.assertLogs(logger, "INFO") as logs:
            rel_id = self.add_relation(self.harness, self.provider)
            self.harness.update_relation_data(
                rel_id,
                self.provider,
                {
                    "version": "v1",
                    "requests": json.dumps(
                        [
                            {
                                "salt": "kkkkkkkk",
                                "request-id": "c759221a6c14c72a",
                                "secret-user": secret.id,
                            }
                        ]
                    ),
                    "data": json.dumps(
                        {
                            "c759221a6c14c72a": {
                                "salt": "kkkkkkkk",
                                "request-id": "c759221a6c14c72a",
                                "resource": DATABASE,
                            }
                        }
                    ),
                },
            )

        # make sure two events were fired
        self.assertEqual(len(logs.output), 2)
        self.assertListEqual(
            [self.harness.charm.get_prefix(log) for log in logs.output],
            ["on_resource_created", "on_cluster1_resource_created"],
        )
        self.assertEqual(self.harness.charm.get_relation_size(logs.output[0]), 1)

        with self.assertLogs(logger, "INFO") as logs:
            self.harness.remove_relation(rel_id)

        # Within the relation broken event the requirer should not show any relation
        self.assertEqual(self.harness.charm.get_relation_size(logs.output[0]), 0)
        self.assertEqual(self.harness.charm.get_prefix(logs.output[0]), "on_relation_broken")


class TestDatabaseRequires(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = DATABASE_RELATION_NAME
    charm = ApplicationCharmDatabase

    app_name = "application"
    provider = "database"

    DATABASE_FIELD = "database"

    def setUp(self):
        self.harness = self.get_harness()
        self.rel_id = self.add_relation(self.harness, self.provider)
        self.harness.begin_with_initial_hooks()

    def test_requires_interface_functions_secrets(self):
        """Check the functionality of each public interface function."""
        interface = self.harness.charm.requirer.interface
        verify_relation_interface_functions(interface, self.rel_id)
        relation = self.harness.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        assert relation

        # Get remote data
        rel_data = interface.repository(self.rel_id, relation.app).get_data()
        assert rel_data == {}

        # Get my data
        rel_data = interface.repository(self.rel_id).get_data()
        assert rel_data == {
            "alias": "cluster1",
            "version": "v1",
            "requests": [
                {
                    "entity-permissions": None,
                    "entity-type": None,
                    "external-node-connectivity": False,
                    "extra-group-roles": None,
                    "extra-user-roles": "CREATEDB,CREATEROLE",
                    "request-id": "c759221a6c14c72a",
                    "resource": "data_platform",
                    "salt": "kkkkkkkk",
                    "secret-mtls": None,
                },
                {
                    "entity-permissions": None,
                    "entity-type": "USER",
                    "external-node-connectivity": False,
                    "extra-group-roles": None,
                    "extra-user-roles": None,
                    "request-id": "9ecfabfbb5258f88",
                    "resource": "",
                    "salt": "xxxxxxxx",
                    "secret-mtls": None,
                },
            ],
        }

    @patch.object(charm, "_on_resource_created")
    def test_on_resource_created_secrets(self, _on_resource_created):
        """Asserts on_resource_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created database.
        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            f"{PROV_SECRET_PREFIX}user": secret.id,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                        }
                    ]
                ),
                "data": json.dumps(
                    {
                        "c759221a6c14c72a": {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "resource": DATABASE,
                        }
                    }
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_resource_created.call_args[0][0]
        assert event.response.secret_user == secret.id
        assert event.response.username.get_secret_value() == "test-username"
        assert event.response.password.get_secret_value() == "test-password"

        assert self.harness.charm.requirer.is_resource_created(
            self.rel_id, event.response.request_id
        )
        assert self.harness.charm.requirer.are_all_resources_created(self.rel_id)

        rel_id = self.add_relation(self.harness, self.provider)

        secret2 = self.harness.charm.app.add_secret(
            {"username": "test-username-2", "password": "test-password-2"}
        )
        self.harness.update_relation_data(
            rel_id,
            self.provider,
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            f"{PROV_SECRET_PREFIX}user": secret2.id,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                        }
                    ]
                ),
                "data": json.dumps(
                    {
                        "c759221a6c14c72a": {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "resource": DATABASE,
                        }
                    }
                ),
            },
        )

        # Assert the correct hook is called.
        assert _on_resource_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_resource_created.call_args[0][0]
        assert event.response.secret_user == secret2.id
        assert event.response.username.get_secret_value() == "test-username-2"
        assert event.response.password.get_secret_value() == "test-password-2"

        assert self.harness.charm.requirer.is_resource_created(rel_id, event.response.request_id)
        assert self.harness.charm.requirer.are_all_resources_created(rel_id)

    @patch.object(charm, "_on_resource_entity_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_on_resource_entity_created_secrets(self, _on_resource_entity_created):
        """Asserts on_resource_entity_created is called when the credentials are set in the relation."""
        secret = self.harness.charm.app.add_secret(
            {"entity-name": "test-username", "entity-password": "test-password"}
        )

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "data": json.dumps(
                    {
                        "9ecfabfbb5258f88": {
                            "salt": "xxxxxxxx",
                            "request-id": "9ecfabfbb5258f88",
                            "entity-type": ENTITY_USER,
                        }
                    }
                ),
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            "salt": "xxxxxxxx",
                            "request-id": "9ecfabfbb5258f88",
                            "secret-entity": secret.id,
                        }
                    ]
                ),
            },
        )

        # Assert the correct hook is called.
        _on_resource_entity_created.assert_called_once()

        # Check that the entity-type, entity-name and entity-password are present in the relation.
        event = _on_resource_entity_created.call_args[0][0]
        assert event.response.secret_entity == secret.id
        assert event.response.entity_name.get_secret_value() == "test-username"
        assert event.response.entity_password.get_secret_value() == "test-password"

        # Reset the mock call count.
        _on_resource_entity_created.reset_mock()

        rel_id = self.add_relation(self.harness, self.provider)

        secret2 = self.harness.charm.app.add_secret({"entity-name": "test-groupname"})

        self.harness.update_relation_data(
            rel_id,
            self.provider,
            {
                "data": json.dumps(
                    {
                        "9ecfabfbb5258f88": {
                            "salt": "xxxxxxxx",
                            "request-id": "9ecfabfbb5258f88",
                            "entity-type": ENTITY_GROUP,
                        }
                    }
                ),
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            "salt": "xxxxxxxx",
                            "request-id": "9ecfabfbb5258f88",
                            "secret-entity": secret2.id,
                        }
                    ]
                ),
            },
        )
        # Assert the correct hook is called.
        _on_resource_entity_created.assert_called_once()

        # Check that the entity-type and entity-name are present in the relation.
        event = _on_resource_entity_created.call_args[0][0]
        assert event.response.secret_entity == secret2.id
        assert event.response.entity_name.get_secret_value() == "test-groupname"
        assert event.response.entity_password is None

    def test_fetch_relation_data_secrets_fields(self):
        # Set user secret for the relation.
        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "version": "v1",
                "requests": json.dumps(
                    [
                        {
                            f"{PROV_SECRET_PREFIX}user": secret.id,
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                        }
                    ]
                ),
            },
        )
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, self.provider, {"somedata": "somevalue"})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        relation = self.harness.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        assert relation
        repository: OpsRepository = self.harness.charm.requirer.interface.repository(
            self.rel_id, relation.app
        )
        data = repository.get_data()
        assert data
        assert data["somedata"] == "somevalue"
        assert data.get("version")
        assert data.get("requests")

        assert repository.get_field("somedata") == "somevalue"
        assert repository.get_field("non-existing") is None
        assert (
            repository.get_secret_field("password", SecretGroup("user"), uri=secret.id)
            == "test-password"
        )

    @pytest.mark.usefixtures("use_caplog")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_fetch_my_relation_data_and_fields_secrets(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, self.app_name, {"somedata": "somevalue"})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        repository: OpsRepository = self.harness.charm.requirer.interface.repository(
            self.rel_id, self.harness.charm.app
        )
        data = repository.get_data()
        assert data == {
            "alias": "cluster1",
            "somedata": "somevalue",
            "version": "v1",
            "requests": [
                {
                    "entity-permissions": None,
                    "entity-type": None,
                    "salt": "kkkkkkkk",
                    "request-id": "c759221a6c14c72a",
                    "resource": "data_platform",
                    "extra-group-roles": None,
                    "extra-user-roles": "CREATEDB,CREATEROLE",
                    "external-node-connectivity": False,
                    "secret-mtls": None,
                },
                {
                    "entity-permissions": None,
                    "salt": "xxxxxxxx",
                    "request-id": "9ecfabfbb5258f88",
                    "resource": "",
                    "entity-type": "USER",
                    "extra-group-roles": None,
                    "extra-user-roles": None,
                    "external-node-connectivity": False,
                    "secret-mtls": None,
                },
            ],
        }

        data = repository.get_field("somedata")
        assert data == "somevalue"

        data = repository.get_field("non-existing-data")
        assert data is None

        data = repository.get_fields("non-existing-data")
        assert data == {}

        self.harness.set_leader(False)
        with self._caplog.at_level(logging.ERROR):
            assert repository.get_field("somedata") is None
            assert (
                "This operation (get_field) can only be performed by the leader unit"
                in self._caplog.text
            )

    @patch.object(charm, "_on_endpoints_changed")
    def test_on_endpoints_changed(self, _on_endpoints_changed):
        """Asserts the correct call to on_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "endpoints": "host1:port,host2:port",
                        }
                    ]
                ),
                "data": json.dumps(
                    {
                        "c759221a6c14c72a": {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "resource": DATABASE,
                        }
                    }
                ),
            },
        )

        # Assert the correct hook is called.
        _on_endpoints_changed.assert_called_once()

        # Check that the endpoints are present in the relation
        # using the requires charm library event.
        event = _on_endpoints_changed.call_args[0][0]
        assert event.response.endpoints == "host1:port,host2:port"

        # Reset the mock call count.
        _on_endpoints_changed.reset_mock()

        # Set the same data in the relation (no change in the endpoints).
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "endpoints": "host1:port,host2:port",
                        }
                    ]
                )
            },
        )

        # Assert the hook was not called again.
        _on_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "endpoints": "host1:port,host2:port,host3:port",
                        }
                    ]
                )
            },
        )

        # Assert the hook is called now.
        _on_endpoints_changed.assert_called_once()

    @patch.object(charm, "_on_read_only_endpoints_changed")
    def test_on_read_only_endpoints_changed(self, _on_read_only_endpoints_changed):
        """Asserts the correct call to on_read_only_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "read-only-endpoints": "host1:port,host2:port",
                        }
                    ]
                ),
                "data": json.dumps(
                    {
                        "c759221a6c14c72a": {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "resource": DATABASE,
                        }
                    }
                ),
            },
        )

        # Assert the correct hook is called.
        _on_read_only_endpoints_changed.assert_called_once()

        # Check that the endpoints are present in the relation
        # using the requires charm library event.
        event = _on_read_only_endpoints_changed.call_args[0][0]
        assert event.response.read_only_endpoints == "host1:port,host2:port"

        # Reset the mock call count.
        _on_read_only_endpoints_changed.reset_mock()

        # Set the same data in the relation (no change in the endpoints).
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "read-only-endpoints": "host1:port,host2:port",
                        }
                    ]
                )
            },
        )

        # Assert the hook was not called again.
        _on_read_only_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "read-only-endpoints": "host1:port,host2:port,host3:port",
                        }
                    ]
                )
            },
        )

        # Assert the hook is called now.
        _on_read_only_endpoints_changed.assert_called_once()

    @patch.object(charm, "_on_resource_created")
    def test_additional_fields_are_accessible(self, _on_resource_created):
        """Asserts additional fields are accessible using the charm library after being set."""
        secret = self.harness.charm.app.add_secret({"tls": "true", "tls-ca": "deadbeef"})
        secret_user = self.harness.charm.app.add_secret(
            {"username": "dead", "password": "beef", "uris": "host1:port,host2:port"}
        )
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "requests": json.dumps(
                    [
                        {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "secret-tls": secret.id,
                            "secret-user": secret_user.id,
                            "version": "1.0",
                        }
                    ]
                ),
                "data": json.dumps(
                    {
                        "c759221a6c14c72a": {
                            "salt": "kkkkkkkk",
                            "request-id": "c759221a6c14c72a",
                            "resource": DATABASE,
                        }
                    }
                ),
            },
        )

        _on_resource_created.assert_called_once()
        event = _on_resource_created.call_args[0][0]

        # Check that the fields are present in the relation
        # using the requires charm library.
        assert event.response.tls.get_secret_value() is True
        assert event.response.tls_ca.get_secret_value() == "deadbeef"
        assert event.response.uris.get_secret_value() == "host1:port,host2:port"
        assert event.response.version == "1.0"

    def test_assign_relation_alias(self):
        """Asserts the correct relation alias is assigned to the relation."""
        unit_name = f"{self.app_name}/0"

        # Reset the alias.
        self.harness.update_relation_data(self.rel_id, unit_name, {"alias": ""})

        # Call the function and check the alias.
        self.harness.charm.requirer._assign_relation_alias(self.rel_id)
        assert (
            self.harness.get_relation_data(self.rel_id, unit_name)["alias"] == CLUSTER_ALIASES[0]
        )

        # Add another relation and check that the second cluster alias was assigned to it.
        second_rel_id = self.add_relation(self.harness, "another-database")

        assert (
            self.harness.get_relation_data(second_rel_id, unit_name)["alias"] == CLUSTER_ALIASES[1]
        )

        # Reset the alias and test again using the function call.
        self.harness.update_relation_data(second_rel_id, unit_name, {"alias": ""})
        self.harness.charm.requirer._assign_relation_alias(second_rel_id)
        assert (
            self.harness.get_relation_data(second_rel_id, unit_name)["alias"] == CLUSTER_ALIASES[1]
        )

    @patch.object(charm, "_on_cluster1_resource_created")
    def test_emit_aliased_event(self, _on_cluster1_resource_created):
        """Asserts the correct custom event is triggered."""
        # Reset the diff/data key in the relation to correctly emit the event.
        self.harness.update_relation_data(self.rel_id, self.app_name, {"data": "{}"})

        # Check that the event wasn't triggered yet.
        _on_cluster1_resource_created.assert_not_called()

        # Call the emit function and assert the desired event is triggered.
        relation = self.harness.charm.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        mock_event = Mock()
        mock_event.app = self.harness.charm.model.get_app(self.app_name)
        mock_event.unit = self.harness.charm.model.get_unit(f"{self.app_name}/0")
        mock_event.relation = relation
        response = ResourceProviderModel.model_validate({})
        self.harness.charm.requirer._emit_aliased_event(mock_event, "resource_created", response)
        _on_cluster1_resource_created.assert_called_once()

    def test_get_relation_alias(self):
        """Asserts the correct relation alias is returned."""
        # Assert the relation got the first cluster alias.
        assert self.harness.charm.requirer._get_relation_alias(self.rel_id) == CLUSTER_ALIASES[0]

    @parameterized.expand([(True,), (False,)])
    def test_resource_events(self, is_leader: bool):
        # Test custom events creation
        # Test that the events are emitted to both the leader
        # and the non-leader units through is_leader parameter.
        secret_user = self.harness.charm.app.add_secret({"username": "dead", "password": "beef"})

        self.harness.set_leader(is_leader)

        # Define the events that need to be emitted.
        # The event key is the event that should have been emitted
        # and the data key is the data that will be updated in the
        # relation databag to trigger that event.
        events = [
            {
                "event": ResourceCreatedEvent,
                "data": {
                    "secret-user": secret_user.id,
                    "endpoints": "host1:port",
                    "read-only-endpoints": "host2:port",
                },
            },
            {
                "event": ResourceEndpointsChangedEvent,
                "data": {
                    "endpoints": "host1:port,host3:port",
                    "read-only-endpoints": "host2:port,host4:port",
                },
            },
            {
                "event": ResourceReadOnlyEndpointsChangedEvent,
                "data": {
                    "read-only-endpoints": "host2:port,host4:port,host5:port",
                },
            },
        ]

        # Define the list of all events that should be checked
        # when something changes in the relation databag.
        all_events = [event["event"] for event in events]

        for event in events:
            # Diff stored in the data field of the relation databag in the previous event.
            # This is important to test the next events in a consistent way.
            previous_event_diff = self.harness.get_relation_data(
                self.rel_id, f"{self.app_name}/0"
            ).get("data", "")

            # Test the event being emitted by the application.
            with capture_events(self.harness.charm, *all_events) as captured_events:
                self.harness.update_relation_data(
                    self.rel_id,
                    self.provider,
                    {
                        "version": "v1",
                        "requests": json.dumps(
                            [
                                {
                                    "salt": "kkkkkkkk",
                                    "request-id": "c759221a6c14c72a",
                                }
                                | event["data"]
                            ]
                        ),
                        "data": json.dumps(
                            {
                                "c759221a6c14c72a": {
                                    "salt": "kkkkkkk",
                                    "request-id": "c759221a6c14c72a",
                                    "resource": DATABASE,
                                }
                            }
                        ),
                    },
                )

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2

            # Check that the events that were emitted are the ones that were expected.
            assert all(
                isinstance(captured_event, event["event"]) for captured_event in captured_events
            )

            # Test that the remote app name is available in the event.
            for captured in captured_events:
                assert captured.app.name == self.provider

            # Reset the diff data to trigger the event again later.
            self.harness.update_relation_data(
                self.rel_id, f"{self.app_name}/0", {"data": previous_event_diff}
            )

            # Test the event being emitted by the unit.
            with capture_events(self.harness.charm, *all_events) as captured_events:
                self.harness.update_relation_data(
                    self.rel_id,
                    f"{self.provider}/0",
                    {
                        "version": "v1",
                        "requests": json.dumps(
                            [
                                {
                                    "salt": "kkkkkkkk",
                                    "request-id": "c759221a6c14c72a",
                                }
                                | event["data"]
                            ]
                        ),
                    },
                )

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2

            # Check that the events that were emitted are the ones that were expected.
            assert all(
                isinstance(captured_event, event["event"]) for captured_event in captured_events
            )

            # Test that the remote unit name is available in the event.
            for captured in captured_events:
                assert captured.unit.name == f"{self.provider}/0"
