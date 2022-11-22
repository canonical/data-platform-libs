# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from abc import ABC, abstractmethod
from typing import Tuple
from unittest.mock import Mock, patch

import pytest
from charms.data_platform_libs.v0.database_requires import (
    DatabaseCreatedEvent,
    DatabaseEndpointsChangedEvent,
    DatabaseEvents,
    DatabaseReadOnlyEndpointsChangedEvent,
    DatabaseRequires,
    Diff,
    KafkaRequires,
)
from charms.harness_extensions.v0.capture_events import capture_events
from ops.charm import CharmBase
from ops.testing import Harness
from parameterized import parameterized

CLUSTER_ALIASES = ["cluster1", "cluster2"]
DATABASE = "data_platform"
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"
DATABASE_RELATION_INTERFACE = "database_client"
DATABASE_RELATION_NAME = "database"
KAFKA_RELATION_INTERFACE = "database_client"
KAFKA_RELATION_NAME = "kafka"
METADATA = f"""
name: application
requires:
  {DATABASE_RELATION_NAME}:
    interface: {DATABASE_RELATION_INTERFACE}
    limit: {len(CLUSTER_ALIASES)}
  {KAFKA_RELATION_NAME}:
    interface: {KAFKA_RELATION_INTERFACE}
"""
TOPIC = "data_platform_topic"


class ApplicationCharmDatabase(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = DatabaseRequires(
            self, DATABASE_RELATION_NAME, DATABASE, EXTRA_USER_ROLES, CLUSTER_ALIASES[:]
        )
        self.framework.observe(self.requirer.on.database_created, self._on_database_created)
        self.framework.observe(self.requirer.on.endpoints_changed, self._on_endpoints_changed)
        self.framework.observe(
            self.requirer.on.read_only_endpoints_changed, self._on_read_only_endpoints_changed
        )
        self.framework.observe(
            self.requirer.on.cluster1_database_created, self._on_cluster1_database_created
        )

    def _on_database_created(self, _) -> None:
        pass

    def _on_endpoints_changed(self, _) -> None:
        pass

    def _on_read_only_endpoints_changed(self, _) -> None:
        pass

    def _on_cluster1_database_created(self, _) -> None:
        pass


class ApplicationCharmKafka(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = KafkaRequires(self, KAFKA_RELATION_NAME, TOPIC, EXTRA_USER_ROLES)
        self.framework.observe(self.requirer.on.topic_created, self._on_topic_created)
        self.framework.observe(
            self.requirer.on.bootstrap_server_changed, self._on_bootstrap_server_changed
        )
        self.framework.observe(self.requirer.on.credentials_changed, self._on_credentials_changed)

    def _on_topic_created(self, _) -> None:
        pass

    def _on_bootstrap_server_changed(self, _) -> None:
        pass

    def _on_credentials_changed(self, _) -> None:
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
            delattr(DatabaseEvents, f"{cluster_alias}_endpoints_changed")
            delattr(DatabaseEvents, f"{cluster_alias}_read_only_endpoints_changed")
        except AttributeError:
            # Ignore the events not existing before the first test.
            pass


class DataRequirerBaseTests(ABC):
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
        mock_event = Mock()
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app(self.app_name)
        local_unit = self.harness.charm.model.get_unit("application/0")
        mock_event.relation.id = self.rel_id
        mock_event.relation.data = {
            mock_event.app: {"username": "test-username", "password": "test-password"},
            local_unit: {},  # Initial empty databag in the local unit.
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.requirer._diff(mock_event)
        assert result == Diff({"username", "password"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.requirer._diff(mock_event)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data["username"] = "test-username-1"
        result = self.harness.charm.requirer._diff(mock_event)
        assert result == Diff(set(), {"username"}, set())

        # Test with deleted data.
        del data["username"]
        del data["password"]
        result = self.harness.charm.requirer._diff(mock_event)
        assert result == Diff(set(), set(), {"username", "password"})


class TestDatabaseRequires(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = DATABASE_RELATION_NAME
    app_name = "database"
    charm = ApplicationCharmDatabase

    def get_harness(self) -> Tuple[Harness, int]:
        harness = Harness(self.charm, meta=self.metadata)
        rel_id = harness.add_relation(DATABASE_RELATION_NAME, "database")
        harness.add_relation_unit(rel_id, "database/0")
        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    @patch.object(charm, "_on_database_created")
    def test_on_database_created(self, _on_database_created):
        """Asserts on_database_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created database.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_database_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert event.username == "test-username"
        assert event.password == "test-password"

    @patch.object(charm, "_on_endpoints_changed")
    def test_on_endpoints_changed(self, _on_endpoints_changed):
        """Asserts the correct call to on_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
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
            self.app_name,
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_endpoints_changed.assert_called_once()

    @patch.object(charm, "_on_read_only_endpoints_changed")
    def test_on_read_only_endpoints_changed(self, _on_read_only_endpoints_changed):
        """Asserts the correct call to on_read_only_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
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
            self.app_name,
            {"read-only-endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_read_only_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"read-only-endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_read_only_endpoints_changed.assert_called_once()

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
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
        relation_data = self.harness.charm.requirer.fetch_relation_data()[self.rel_id]
        assert relation_data["replset"] == "rs0"
        assert relation_data["tls"] == "True"
        assert relation_data["tls-ca"] == "Canonical"
        assert relation_data["uris"] == "host1:port,host2:port"
        assert relation_data["version"] == "1.0"

    @patch.object(charm, "_on_database_created")
    def test_fields_are_accessible_through_event(self, _on_database_created):
        """Asserts fields are accessible through the requires charm library event."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
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
        self.harness.update_relation_data(self.rel_id, "application/0", {"alias": ""})

        # Call the function and check the alias.
        self.harness.charm.requirer._assign_relation_alias(self.rel_id)
        assert (
            self.harness.get_relation_data(self.rel_id, "application/0")["alias"]
            == CLUSTER_ALIASES[0]
        )

        # Add another relation and check that the second cluster alias was assigned to it.
        second_rel_id = self.harness.add_relation(DATABASE_RELATION_NAME, "another-database")
        self.harness.add_relation_unit(second_rel_id, "another-database/0")
        assert (
            self.harness.get_relation_data(second_rel_id, "application/0")["alias"]
            == CLUSTER_ALIASES[1]
        )

        # Reset the alias and test again using the function call.
        self.harness.update_relation_data(second_rel_id, "application/0", {"alias": ""})
        self.harness.charm.requirer._assign_relation_alias(second_rel_id)
        assert (
            self.harness.get_relation_data(second_rel_id, "application/0")["alias"]
            == CLUSTER_ALIASES[1]
        )

    @patch.object(charm, "_on_cluster1_database_created")
    def test_emit_aliased_event(self, _on_cluster1_database_created):
        """Asserts the correct custom event is triggered."""
        # Reset the diff/data key in the relation to correctly emit the event.
        self.harness.update_relation_data(self.rel_id, "application", {"data": "{}"})

        # Check that the event wasn't triggered yet.
        _on_cluster1_database_created.assert_not_called()

        # Call the emit function and assert the desired event is triggered.
        relation = self.harness.charm.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        mock_event = Mock()
        mock_event.app = self.harness.charm.model.get_app("application")
        mock_event.unit = self.harness.charm.model.get_unit("application/0")
        mock_event.relation = relation
        self.harness.charm.requirer._emit_aliased_event(mock_event, "database_created")
        _on_cluster1_database_created.assert_called_once()

    def test_get_relation_alias(self):
        """Asserts the correct relation alias is returned."""
        # Assert the relation got the first cluster alias.
        assert self.harness.charm.requirer._get_relation_alias(self.rel_id) == CLUSTER_ALIASES[0]

    @parameterized.expand([(True,), (False,)])
    def test_database_events(self, is_leader: bool):
        # Test custom events creation
        # Test that the events are emitted to both the leader
        # and the non-leader units through is_leader parameter.
        self.harness.set_leader(is_leader)

        # Define the events that need to be emitted.
        # The event key is the event that should have been emitted
        # and the data key is the data that will be updated in the
        # relation databag to trigger that event.
        events = [
            {
                "event": DatabaseCreatedEvent,
                "data": {
                    "username": "test-username",
                    "password": "test-password",
                    "endpoints": "host1:port",
                    "read-only-endpoints": "host2:port",
                },
            },
            {
                "event": DatabaseEndpointsChangedEvent,
                "data": {
                    "endpoints": "host1:port,host3:port",
                    "read-only-endpoints": "host2:port,host4:port",
                },
            },
            {
                "event": DatabaseReadOnlyEndpointsChangedEvent,
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
            previous_event_diff = self.harness.get_relation_data(self.rel_id, "application/0").get(
                "data"
            )

            # Test the event being emitted by the application.
            with capture_events(self.harness.charm, *all_events) as captured_events:
                self.harness.update_relation_data(self.rel_id, "database", event["data"])

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2

            # Check that the events that were emitted are the ones that were expected.
            assert all(
                isinstance(captured_event, event["event"]) for captured_event in captured_events
            )

            # Test that the remote app name is available in the event.
            for captured in captured_events:
                assert captured.app.name == "database"

            # Reset the diff data to trigger the event again later.
            self.harness.update_relation_data(
                self.rel_id, "application/0", {"data": previous_event_diff}
            )

            # Test the event being emitted by the unit.
            with capture_events(self.harness.charm, *all_events) as captured_events:
                self.harness.update_relation_data(self.rel_id, "database/0", event["data"])

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2

            # Check that the events that were emitted are the ones that were expected.
            assert all(
                isinstance(captured_event, event["event"]) for captured_event in captured_events
            )

            # Test that the remote unit name is available in the event.
            for captured in captured_events:
                assert captured.unit.name == "database/0"


class TestKakfaRequires(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = KAFKA_RELATION_NAME
    app_name = "kafka"
    charm = ApplicationCharmKafka

    def get_harness(self) -> Tuple[Harness, int]:
        harness = Harness(self.charm, meta=self.metadata)
        rel_id = harness.add_relation(KAFKA_RELATION_NAME, self.app_name)
        harness.add_relation_unit(rel_id, f"{self.app_name}/0")
        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    @patch.object(charm, "_on_topic_created")
    def test_on_topic_created(self, _on_topic_created):
        """Asserts on_topic_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created topic.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_topic_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert event.username == "test-username"
        assert event.password == "test-password"

    @patch.object(charm, "_on_bootstrap_server_changed")
    def test_on_bootstrap_server_changed(self, _on_bootstrap_server_changed):
        """Asserts the correct call to _on_bootstrap_server_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the correct hook is called.
        _on_bootstrap_server_changed.assert_called_once()

        # Check that the endpoints are present in the relation
        # using the requires charm library event.
        event = _on_bootstrap_server_changed.call_args[0][0]
        assert event.bootstrap_server == "host1:port,host2:port"

        # Reset the mock call count.
        _on_bootstrap_server_changed.reset_mock()

        # Set the same data in the relation (no change in the endpoints).
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_bootstrap_server_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_bootstrap_server_changed.assert_called_once()

    @patch.object(charm, "_on_credentials_changed")
    def test_on_credentials_changed(self, _on_credentials_changed):
        """Asserts the correct call to on_credentials_changed."""
        # Setup initial username and password
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"username": "username", "password": "password"},
        )
        # Simulate changing password with the relation data.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"password": "new_password"},
        )

        # Assert the correct hook is called.
        _on_credentials_changed.assert_called_once()

        # Check that the password is present in the relation
        # using the requires charm library event.
        event = _on_credentials_changed.call_args[0][0]
        assert event.password == "new_password"

        # Reset the mock call count.
        _on_credentials_changed.reset_mock()

        # Set the same data in the relation (no change in the password).
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"password": "new_password"},
        )

        # Assert the hook was not called again.
        _on_credentials_changed.assert_not_called()

        # Then, change the password in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {"password": "new_password-2"},
        )

        # Assert the hook is called now.
        _on_credentials_changed.assert_called_once()

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {
                "tls": "True",
                "tls-ca": "Canonical",
                "version": "1.0",
                "zookeeper-uris": "host1:port,host2:port",
                "consumer-group-prefix": "pr1,pr2",
            },
        )

        # Check that the fields are present in the relation
        # using the requires charm library.
        relation_data = self.harness.charm.requirer.fetch_relation_data()[self.rel_id]
        assert relation_data["tls"] == "True"
        assert relation_data["tls-ca"] == "Canonical"
        assert relation_data["version"] == "1.0"
        assert relation_data["zookeeper-uris"] == "host1:port,host2:port"
        assert relation_data["consumer-group-prefix"] == "pr1,pr2"

    @patch.object(charm, "_on_topic_created")
    def test_fields_are_accessible_through_event(self, _on_topic_created):
        """Asserts fields are accessible through the requires charm library event."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.app_name,
            {
                "username": "test-username",
                "password": "test-password",
                "endpoints": "host1:port,host2:port",
                "tls": "True",
                "tls-ca": "Canonical",
                "zookeeper-uris": "h1:port,h2:port",
                "consumer-group-prefix": "pr1,pr2",
            },
        )

        # Check that the fields are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert event.username == "test-username"
        assert event.password == "test-password"
        assert event.bootstrap_server == "host1:port,host2:port"
        assert event.tls == "True"
        assert event.tls_ca == "Canonical"
        assert event.zookeeper_uris == "h1:port,h2:port"
        assert event.consumer_group_prefix == "pr1,pr2"
