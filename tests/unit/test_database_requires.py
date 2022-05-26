# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import unittest
from unittest.mock import Mock, patch

from charms.data_platform_libs.v0.database_requires import DatabaseRequires
from ops.charm import CharmBase
from ops.testing import Harness

DATABASE = "data_platform"
RELATION_INTERFACE = "database-client"
RELATION_NAME = "database"
METADATA = f"""
name: application
requires:
  {RELATION_NAME}:
    interface: {RELATION_INTERFACE}
"""


class ApplicationCharm(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.database = DatabaseRequires(
            self,
            RELATION_NAME,
        )
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_endpoints_changed)
        self.framework.observe(
            self.database.on.read_only_endpoints_changed, self._on_read_only_endpoints_changed
        )

    def _on_database_created(self, _) -> None:
        pass

    def _on_endpoints_changed(self, _) -> None:
        pass

    def _on_read_only_endpoints_changed(self, _) -> None:
        pass


class TestDatabaseRequires(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(ApplicationCharm, meta=METADATA)
        self.addCleanup(self.harness.cleanup)

        # Set up the initial relation and hooks.
        self.rel_id = self.harness.add_relation(RELATION_NAME, "database")
        self.harness.add_relation_unit(self.rel_id, "database/0")
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

    def test_set_database(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        # Set the database name in the relation using the requires charm library.
        self.harness.charm.database.set_database(DATABASE)

        # Check that the database name is present in the relation.
        assert self.harness.get_relation_data(self.rel_id, "application")["database"] == DATABASE

    @patch.object(ApplicationCharm, "_on_database_created")
    def test_on_database_created(self, _on_database_created):
        """Asserts on_database_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created database.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {
                "credentials": json.dumps(
                    {"username": "test-username", "password": "test-password"}
                )
            },
        )

        # Check that the username and the password are present in the relation
        # using the requires charm library.
        assert self.harness.charm.database.username == "test-username"
        assert self.harness.charm.database.password == "test-password"

        # Assert the correct hook is called.
        _on_database_created.assert_called_once()

    @patch.object(ApplicationCharm, "_on_endpoints_changed")
    def test_on_endpoints_changed(self, _on_endpoints_changed):
        """Asserts the correct call to on_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"endpoints": "host1:port,host2:port"},
        )

        # Check that the endpoints are present in the relation
        # using the requires charm library.
        assert self.harness.charm.database.endpoints == "host1:port,host2:port"

        # Assert the correct hook is called.
        _on_endpoints_changed.assert_called_once()

        # Reset the mock call count.
        _on_endpoints_changed.reset_mock()

        # Set the same data in the relation (no change in the endpoints).
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_endpoints_changed.assert_called_once()

    @patch.object(ApplicationCharm, "_on_read_only_endpoints_changed")
    def test_on_read_only_endpoints_changed(self, _on_read_only_endpoints_changed):
        """Asserts the correct call to on_read_only_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"read-only-endpoints": "host1:port,host2:port"},
        )

        # Check that the endpoints are present in the relation
        # using the requires charm library.
        assert self.harness.charm.database.read_only_endpoints == "host1:port,host2:port"

        # Assert the correct hook is called.
        _on_read_only_endpoints_changed.assert_called_once()

        # Reset the mock call count.
        _on_read_only_endpoints_changed.reset_mock()

        # Set the same data in the relation (no change in the endpoints).
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"read-only-endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_read_only_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"read-only-endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_read_only_endpoints_changed.assert_called_once()

    def test_set_extra_user_roles(self):
        """Asserts that the extra user roles are in the relation databag when they're requested."""
        # Set the extra roles that the user need using the provides charm library.
        self.harness.charm.database.set_extra_user_roles("CREATEDB,CREATEROLE")

        # Check that the extra user roles are present in the relation.
        assert (
            self.harness.get_relation_data(self.rel_id, "application")["extra-user-roles"]
            == "CREATEDB,CREATEROLE"
        )

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {
                "replset": "rs0",
                "tls": "True",
                "tls-ca": "Canonical",
                "uris": "host1:port,host2:port",
                "version": "1.0",
            },
        )

        # Check that the fields are present in the relation
        # using the requires charm library.
        assert self.harness.charm.database.replset == "rs0"
        assert self.harness.charm.database.tls == "True"
        assert self.harness.charm.database.tls_ca == "Canonical"
        assert self.harness.charm.database.uris == "host1:port,host2:port"
        assert self.harness.charm.database.version == "1.0"
