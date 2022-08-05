# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from unittest.mock import Mock, PropertyMock, patch

import pytest
from charms.data_platform_libs.v0.database_requires import (
    DatabaseCreatedEvent,
    DatabaseDepartedEvent,
    DatabaseEndpointsChangedEvent,
    DatabaseEvents,
    DatabaseReadOnlyEndpointsChangedEvent,
    DatabaseRequires,
    Diff,
)
from charms.harness_extensions.v0.capture_events import capture_events
from ops.charm import CharmBase, RelationDepartedEvent
from ops.testing import Harness
from parameterized import parameterized

CLUSTER_ALIASES = ["cluster1", "cluster2"]
DATABASE = "data_platform"
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"
RELATION_INTERFACE = "database_client"
RELATION_NAME = "database"
METADATA = f"""
name: application
requires:
  {RELATION_NAME}:
    interface: {RELATION_INTERFACE}
    limit: {len(CLUSTER_ALIASES)}
"""


class ApplicationCharm(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.database = DatabaseRequires(
            self, RELATION_NAME, DATABASE, EXTRA_USER_ROLES, CLUSTER_ALIASES[:]
        )
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(self.database.on.endpoints_changed, self._on_endpoints_changed)
        self.framework.observe(
            self.database.on.read_only_endpoints_changed, self._on_read_only_endpoints_changed
        )
        self.framework.observe(
            self.database.on.cluster1_database_created, self._on_cluster1_database_created
        )

    def _on_database_created(self, _) -> None:
        pass

    def _on_endpoints_changed(self, _) -> None:
        pass

    def _on_read_only_endpoints_changed(self, _) -> None:
        pass

    def _on_cluster1_database_created(self, _) -> None:
        pass


@pytest.fixture(autouse=True)
def reset_aliases():
    """Fixture that runs before each test to delete the custom events created for the aliases.

    This is needed because the events are created again in the next test,
    which causes an error related to duplicated events.
    """
    for cluster_alias in CLUSTER_ALIASES:
        try:
            delattr(DatabaseEvents, f"{cluster_alias}_database_created")
            delattr(DatabaseEvents, f"{cluster_alias}_database_departed")
            delattr(DatabaseEvents, f"{cluster_alias}_endpoints_changed")
            delattr(DatabaseEvents, f"{cluster_alias}_read_only_endpoints_changed")
        except AttributeError:
            # Ignore the events not existing before the first test.
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
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app("database")
        local_unit = self.harness.charm.model.get_unit("application/0")
        mock_event.relation.id = self.rel_id
        mock_event.relation.data = {
            mock_event.app: {"username": "test-username", "password": "test-password"},
            local_unit: {},  # Initial empty databag in the local unit.
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.database._diff(mock_event)
        assert result == Diff({"username", "password"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.database._diff(mock_event)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data["username"] = "test-username-1"
        result = self.harness.charm.database._diff(mock_event)
        assert result == Diff(set(), {"username"}, set())

        # Test with deleted data.
        del data["username"]
        del data["password"]
        result = self.harness.charm.database._diff(mock_event)
        assert result == Diff(set(), set(), {"username", "password"})

    @patch.object(ApplicationCharm, "_on_database_created")
    def test_on_database_created(self, _on_database_created):
        """Asserts on_database_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created database.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_database_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert event.username == "test-username"
        assert event.password == "test-password"

    def test_on_relation_departed(self):
        """Test that the right events are emitted and the departing unit field is available."""
        # Workaround ops.testing not setting `departing_unit` in v1.5.0
        # ref https://github.com/canonical/operator/pull/790
        with patch.object(
            RelationDepartedEvent, "departing_unit", new_callable=PropertyMock
        ) as mock_departing_unit:
            # Mock the departing unit.
            departing_unit = self.harness.model.get_unit("application/0")
            mock_departing_unit.return_value = departing_unit

            # Add the unit to the relation.
            self.harness.add_relation_unit(self.rel_id, departing_unit.name)

            # Remove a unit from the relation and capture the emitted event.
            with capture_events(self.harness.charm, DatabaseDepartedEvent) as captured_events:
                self.harness.remove_relation_unit(self.rel_id, departing_unit.name)

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2
            for captured in captured_events:
                assert captured.app.name == departing_unit.app.name
                assert captured.unit.name == departing_unit.name
                assert captured.departing_unit == departing_unit

    @patch.object(ApplicationCharm, "_on_endpoints_changed")
    def test_on_endpoints_changed(self, _on_endpoints_changed):
        """Asserts the correct call to on_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the correct hook is called.
        _on_endpoints_changed.assert_called_once()

        # Check that the endpoints are present in the relation
        # using the requires charm library event.
        event = _on_endpoints_changed.call_args[0][0]
        assert event.endpoints == "host1:port,host2:port"

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

        # Assert the correct hook is called.
        _on_read_only_endpoints_changed.assert_called_once()

        # Check that the endpoints are present in the relation
        # using the requires charm library event.
        event = _on_read_only_endpoints_changed.call_args[0][0]
        assert event.read_only_endpoints == "host1:port,host2:port"

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
        relation_data = self.harness.charm.database.fetch_relation_data()[self.rel_id]
        assert relation_data["replset"] == "rs0"
        assert relation_data["tls"] == "True"
        assert relation_data["tls-ca"] == "Canonical"
        assert relation_data["uris"] == "host1:port,host2:port"
        assert relation_data["version"] == "1.0"

    @patch.object(ApplicationCharm, "_on_database_created")
    def test_fields_are_accessible_through_event(self, _on_database_created):
        """Asserts fields are accessible through the requires charm library event."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            "database",
            {
                "username": "test-username",
                "password": "test-password",
                "endpoints": "host1:port,host2:port",
                "read-only-endpoints": "host1:port,host2:port",
                "replset": "rs0",
                "tls": "True",
                "tls-ca": "Canonical",
                "uris": "host1:port,host2:port",
                "version": "1.0",
            },
        )

        # Check that the fields are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert event.username == "test-username"
        assert event.password == "test-password"
        assert event.endpoints == "host1:port,host2:port"
        assert event.read_only_endpoints == "host1:port,host2:port"
        assert event.replset == "rs0"
        assert event.tls == "True"
        assert event.tls_ca == "Canonical"
        assert event.uris == "host1:port,host2:port"
        assert event.version == "1.0"

    def test_assign_relation_alias(self):
        """Asserts the correct relation alias is assigned to the relation."""
        # Reset the alias.
        self.harness.update_relation_data(self.rel_id, "application/0", {"alias": None})

        # Call the function and check the alias.
        self.harness.charm.database._assign_relation_alias(self.rel_id)
        assert (
            self.harness.get_relation_data(self.rel_id, "application/0")["alias"]
            == CLUSTER_ALIASES[0]
        )

        # Add another relation and check that the second cluster alias was assigned to it.
        second_rel_id = self.harness.add_relation(RELATION_NAME, "database")
        self.harness.add_relation_unit(second_rel_id, "another-database/0")
        assert (
            self.harness.get_relation_data(second_rel_id, "application/0")["alias"]
            == CLUSTER_ALIASES[1]
        )

        # Reset the alias and test again using the function call.
        self.harness.update_relation_data(second_rel_id, "application/0", {"alias": None})
        self.harness.charm.database._assign_relation_alias(second_rel_id)
        assert (
            self.harness.get_relation_data(second_rel_id, "application/0")["alias"]
            == CLUSTER_ALIASES[1]
        )

    @patch.object(ApplicationCharm, "_on_cluster1_database_created")
    def test_emit_aliased_event(self, _on_cluster1_database_created):
        """Asserts the correct custom event is triggered."""
        # Reset the diff/data key in the relation to correctly emit the event.
        self.harness.update_relation_data(self.rel_id, "application", {"data": "{}"})

        # Check that the event wasn't triggered yet.
        _on_cluster1_database_created.assert_not_called()

        # Call the emit function and assert the desired event is triggered.
        relation = self.harness.charm.model.get_relation(RELATION_NAME, self.rel_id)
        mock_event = Mock()
        mock_event.app = self.harness.charm.model.get_app("application")
        mock_event.unit = self.harness.charm.model.get_unit("application/0")
        mock_event.relation = relation
        self.harness.charm.database._emit_aliased_event(mock_event, "database_created")
        _on_cluster1_database_created.assert_called_once()

    def test_get_relation_alias(self):
        """Asserts the correct relation alias is returned."""
        # Assert the relation got the first cluster alias.
        assert self.harness.charm.database._get_relation_alias(self.rel_id) == CLUSTER_ALIASES[0]

    @parameterized.expand([(True,), (False,)])
    def test_database_events(self, is_leader: bool):
        # Test custom events creation
        # Test that the events are emitted to both the leader
        # and the non-leader units through is_leader parameter.
        self.harness.set_leader(is_leader)

        events = [
            {
                "event": DatabaseCreatedEvent,
                "data": {"username": "test-username", "password": "test-password"},
            },
            {
                "event": DatabaseEndpointsChangedEvent,
                "data": {"endpoints": "host1:port,host2:port"},
            },
            {
                "event": DatabaseReadOnlyEndpointsChangedEvent,
                "data": {"read-only-endpoints": "host1:port,host2:port"},
            },
        ]

        for event in events:
            # Test the event being emitted by the application.
            with capture_events(self.harness.charm, event["event"]) as captured_events:
                self.harness.update_relation_data(self.rel_id, "database", event["data"])

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2
            for captured in captured_events:
                assert captured.app.name == "database"

            # Reset the diff data to trigger the event again later.
            self.harness.update_relation_data(self.rel_id, "application/0", {"data": "{}"})

            # Test the event being emitted by the unit.
            with capture_events(self.harness.charm, event["event"]) as captured_events:
                self.harness.update_relation_data(self.rel_id, "database/0", event["data"])

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2
            for captured in captured_events:
                assert captured.unit.name == "database/0"
