# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import unittest
from unittest.mock import Mock, patch

from charms.data_platform_libs.v0.database_provides import DatabaseProvides
from ops.charm import CharmBase
from ops.testing import Harness

DATABASE = "data_platform"
RELATION_INTERFACE = "database-client"
RELATION_NAME = "database"
METADATA = f"""
name: database
provides:
  {RELATION_NAME}:
    interface: {RELATION_INTERFACE}
"""


class DatabaseCharm(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.database = DatabaseProvides(
            self,
            RELATION_NAME,
        )
        self.framework.observe(self.database.on.database_requested, self._on_database_requested)
        self.framework.observe(
            self.database.on.extra_user_roles_requested, self._on_extra_user_roles_requested
        )

    def _on_database_requested(self, _) -> None:
        pass

    def _on_extra_user_roles_requested(self, _) -> None:
        pass


class TestDatabaseProvides(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(DatabaseCharm, meta=METADATA)
        self.addCleanup(self.harness.cleanup)

        # Set up the initial relation and hooks.
        self.rel_id = self.harness.add_relation(RELATION_NAME, "application")
        self.harness.add_relation_unit(self.rel_id, "application/0")
        self.harness.set_leader(True)
        self.harness.begin_with_initial_hooks()

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        mock_event = Mock()
        # Set the initial data for the event.
        mock_event.relation.data = {
            mock_event.app: {"username": "test-username", "password": "test-password"}
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.database._diff(mock_event)
        assert result == {"added": {"username", "password"}, "changed": set(), "deleted": set()}

        # Test with the same data.
        result = self.harness.charm.database._diff(mock_event)
        assert result == {"added": set(), "changed": set(), "deleted": set()}

        # Test with changed data.
        data["username"] = "test-username-1"
        result = self.harness.charm.database._diff(mock_event)
        assert result == {"added": set(), "changed": {"username"}, "deleted": set()}

        # Test with deleted data.
        del data["username"]
        del data["password"]
        result = self.harness.charm.database._diff(mock_event)
        assert result == {"added": set(), "changed": set(), "deleted": {"username", "password"}}

    @patch.object(DatabaseCharm, "_on_database_requested")
    def test_on_database_requested(self, _on_database_requested):
        """Asserts that the correct hook is called when a new database is requested."""
        # Simulate the request of a new database.
        self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})

        # Assert the database name is accessible in the providers charm library.
        assert self.harness.charm.database.database == DATABASE

        # Assert the correct hook is called.
        _on_database_requested.assert_called_once()

    def test_set_credentials(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.database.set_credentials("test-username", "test-password")

        # Check that the database name is present in the relation.
        assert json.loads(
            self.harness.get_relation_data(self.rel_id, "database")["credentials"]
        ) == {"username": "test-username", "password": "test-password"}

    def test_set_endpoints(self):
        """Asserts that the endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.database.set_endpoints("host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        assert (
            self.harness.get_relation_data(self.rel_id, "database")["endpoints"]
            == "host1:port,host2:port"
        )

    def test_set_read_only_endpoints(self):
        """Asserts that the read only endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.database.set_read_only_endpoints("host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        assert (
            self.harness.get_relation_data(self.rel_id, "database")["read-only-endpoints"]
            == "host1:port,host2:port"
        )

    def test_set_additional_fields(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.database.set_replset("rs0")
        self.harness.charm.database.set_tls("True")
        self.harness.charm.database.set_tls_ca("Canonical")
        self.harness.charm.database.set_uris("host1:port,host2:port")
        self.harness.charm.database.set_version("1.0")

        # Check that the additional fields are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, "database") == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "replset": "rs0",
            "tls": "True",
            "tls_ca": "Canonical",
            "uris": "host1:port,host2:port",
            "version": "1.0",
        }

    @patch.object(DatabaseCharm, "_on_extra_user_roles_requested")
    def test_on_extra_user_roles_requested(self, _on_extra_user_roles_requested):
        """Asserts that the correct hook is called when extra user roles are requested."""
        # Set the username in the relation databag (like after a database is created).
        self.harness.update_relation_data(
            self.rel_id, "database", {"credentials": json.dumps({"username": "test-user"})}
        )
        # Simulate the request of extra user roles.
        self.harness.update_relation_data(
            self.rel_id, "application", {"extra-user-roles": "CREATEDB,CREATEROLE"}
        )

        # Assert the extra user roles (and the user) are accessible in the providers charm library.
        assert self.harness.charm.database.extra_user_roles == "CREATEDB,CREATEROLE"
        assert self.harness.charm.database.username == "test-user"

        # Assert the correct hook is called.
        _on_extra_user_roles_requested.assert_called_once()
