# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import re
import unittest
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Tuple, Type
from unittest.mock import Mock, patch

import psycopg
import pytest
from ops import JujuVersion
from ops.charm import CharmBase
from ops.testing import Harness
from parameterized import parameterized

from charms.data_platform_libs.v1.data_interfaces import (
    AuthenticationEvent,
    DatabaseCreatedEvent,
    DatabaseEndpointsChangedEvent,
    DatabaseProvides,
    DatabaseReadOnlyEndpointsChangedEvent,
    DatabaseRequestedEvent,
    DatabaseRequires,
    DatabaseRequiresEvents,
    Diff,
    IndexRequestedEvent,
    KafkaProvides,
    KafkaRequires,
    OpenSearchProvides,
    OpenSearchRequires,
    TopicRequestedEvent,
)
from charms.harness_extensions.v0.capture_events import capture, capture_events

logger = getLogger(__name__)

DATABASE = "data_platform"
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"
DATABASE_RELATION_INTERFACE = "database_client"
DATABASE_RELATION_NAME = "database"
DATABASE_METADATA = f"""
name: database
provides:
  {DATABASE_RELATION_NAME}:
    interface: {DATABASE_RELATION_INTERFACE}
"""

TOPIC = "data_platform_topic"
WILDCARD_TOPIC = "*"
KAFKA_RELATION_INTERFACE = "kafka_client"
KAFKA_RELATION_NAME = "kafka"
KAFKA_METADATA = f"""
name: kafka
provides:
  {KAFKA_RELATION_NAME}:
    interface: {KAFKA_RELATION_INTERFACE}
"""

INDEX = "data_platform_index"
OPENSEARCH_RELATION_INTERFACE = "opensearch_client"
OPENSEARCH_RELATION_NAME = "opensearch"
OPENSEARCH_METADATA = f"""
name: opensearch
provides:
  {OPENSEARCH_RELATION_NAME}:
    interface: {OPENSEARCH_RELATION_INTERFACE}
"""


class DatabaseCharm(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = DatabaseProvides(
            self,
            DATABASE_RELATION_NAME,
        )
        self.framework.observe(self.provider.on.database_requested, self._on_database_requested)

    def _on_database_requested(self, _) -> None:
        pass


class KafkaCharm(CharmBase):
    """Mock Kafka charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = KafkaProvides(
            self,
            KAFKA_RELATION_NAME,
        )
        self.framework.observe(self.provider.on.topic_requested, self._on_topic_requested)

    def _on_topic_requested(self, _) -> None:
        pass


class OpenSearchCharm(CharmBase):
    """Mock Opensearch charm to use in unit tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = OpenSearchProvides(
            self,
            OPENSEARCH_RELATION_NAME,
        )
        self.framework.observe(self.provider.on.index_requested, self._on_index_requested)

    def _on_index_requested(self, _) -> None:
        pass


class DataProvidesBaseTests(ABC):
    SECRET_FIELDS = "username password tls tls-ca endpoints uris"

    @abstractmethod
    def get_harness(self) -> Tuple[Harness, int]:
        pass

    def setUp(self):
        self.harness, self.rel_id = self.get_harness()

    def tearDown(self) -> None:
        self.harness.cleanup()

    def setup_secrets_if_needed(self, harness, rel_id):
        if JujuVersion.from_environ().has_secrets:
            harness.update_relation_data(
                rel_id, "application", {"secret_fields": self.SECRET_FIELDS}
            )

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        mock_event = Mock()
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app(self.app_name)
        mock_event.relation.id = self.rel_id
        mock_event.relation.data = {
            mock_event.app: {"username": "test-username", "password": "test-password"}
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff({"username", "password"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data["username"] = "test-username-1"
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff(set(), {"username"}, set())

        # Test with deleted data.
        del data["username"]
        del data["password"]
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff(set(), set(), {"username", "password"})

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_set_credentials(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.provider.set_credentials(self.rel_id, "test-username", "test-password")

        # Check that the credentials are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, self.app_name) == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "username": "test-username",
            "password": "test-password",
        }

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_credentials_secrets(self):
        """Asserts that credentials are set up as secrets if possible."""
        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.provider.set_credentials(self.rel_id, "test-username", "test-password")

        # Check that the credentials are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, self.app_name)["data"] == (
            '{"secret_fields": "'
            + f"{self.SECRET_FIELDS}"
            + '"}'  # Data is the diff stored between multiple relation changed events.   # noqa
        )
        secret_id = self.harness.get_relation_data(self.rel_id, self.app_name)["secret-user"]
        assert secret_id

        secret = self.harness.charm.model.get_secret(id=secret_id)
        assert secret.get_content() == {"username": "test-username", "password": "test-password"}

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_credentials_secrets_provides_juju3_requires_juju2(self):
        """Asserts that the databag is used if one side of the relation is on Juju2."""
        self.harness.update_relation_data(self.rel_id, "application", {"secret_fields": ""})
        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.provider.set_credentials(self.rel_id, "test-username", "test-password")

        # Check that the credentials are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, self.app_name) == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "username": "test-username",
            "password": "test-password",
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

        # Juju 3 - specific setup
        self.setup_secrets_if_needed(harness, rel_id)

        harness.add_relation_unit(rel_id, "application/0")
        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    @patch.object(DatabaseCharm, "_on_database_requested")
    def emit_database_requested_event(self, _on_database_requested):
        # Emit the database requested event.
        relation = self.harness.charm.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        application = self.harness.charm.model.get_app("database")
        self.harness.charm.provider.on.database_requested.emit(relation, application)
        return _on_database_requested.call_args[0][0]

    @patch.object(DatabaseCharm, "_on_database_requested")
    def test_on_database_requested(self, _on_database_requested):
        """Asserts that the correct hook is called when a new database is requested."""
        # Simulate the request of a new database plus extra user roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {"database": DATABASE, "extra-user-roles": EXTRA_USER_ROLES},
        )

        # Assert the correct hook is called.
        _on_database_requested.assert_called_once()

        # Assert the database name and the extra user roles
        # are accessible in the providers charm library event.
        event = _on_database_requested.call_args[0][0]
        assert event.database == DATABASE
        assert event.extra_user_roles == EXTRA_USER_ROLES

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_set_endpoints(self):
        """Asserts that the endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.provider.set_endpoints(self.rel_id, "host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        assert (
            self.harness.get_relation_data(self.rel_id, "database")["endpoints"]
            == "host1:port,host2:port"
        )

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_endpoints_secrets(self):
        """Asserts that the endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.provider.set_endpoints(self.rel_id, "host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        secret_uri = self.harness.get_relation_data(self.rel_id, self.app_name)["secret-endpoints"]
        secret = self.harness.charm.model.get_secret(id=secret_uri)
        assert secret.get_content() == {"endpoints": "host1:port,host2:port"}

    def test_set_read_only_endpoints(self):
        """Asserts that the read only endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.provider.set_read_only_endpoints(self.rel_id, "host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        assert (
            self.harness.get_relation_data(self.rel_id, "database")["read-only-endpoints"]
            == "host1:port,host2:port"
        )

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_set_additional_fields(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_replset(self.rel_id, "rs0")
        self.harness.charm.provider.set_tls(self.rel_id, "True")
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")
        self.harness.charm.provider.set_uris(self.rel_id, "host1:port,host2:port")
        self.harness.charm.provider.set_version(self.rel_id, "1.0")

        # Check that the additional fields are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, "database") == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "replset": "rs0",
            "tls": "True",
            "tls-ca": "Canonical",
            "uris": "host1:port,host2:port",
            "version": "1.0",
        }

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_additional_fields_secrets(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_replset(self.rel_id, "rs0")
        self.harness.charm.provider.set_tls(self.rel_id, "True")
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")
        self.harness.charm.provider.set_uris(self.rel_id, "host1:port,host2:port")
        self.harness.charm.provider.set_version(self.rel_id, "1.0")

        # Check that the additional fields are present in the relation.
        relation_data = self.harness.get_relation_data(self.rel_id, "database")
        secret_id = relation_data.pop("secret-tls")
        assert secret_id

        relation_data == {
            "data": '{"secret_fields": "'
            + f"{self.SECRET_FIELDS}"
            + '"}',  # Data is the diff stored between multiple relation changed events.   # noqa
            "replset": "rs0",
            "version": "1.0",
            "uris": "host1:port,host2:port",
        }

        secret = self.harness.charm.model.get_secret(id=secret_id)
        assert secret.get_content() == {
            "tls": "True",
            "tls-ca": "Canonical",
        }

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_fetch_relation_data(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"database": DATABASE}}

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_fetch_relation_data_secrets(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"database": DATABASE, "secret_fields": self.SECRET_FIELDS}}

    def test_database_requested_event(self):
        # Test custom event creation

        # Test the event being emitted by the application.
        with capture(self.harness.charm, DatabaseRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})
        assert captured.event.app.name == "application"

        # Reset the diff data to trigger the event again later.
        self.harness.update_relation_data(self.rel_id, "database", {"data": "{}"})

        # Test the event being emitted by the unit.
        with capture(self.harness.charm, DatabaseRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application/0", {"database": DATABASE})
        assert captured.event.unit.name == "application/0"


class TestKafkaProvides(DataProvidesBaseTests, unittest.TestCase):
    metadata = KAFKA_METADATA
    relation_name = KAFKA_RELATION_NAME
    app_name = "kafka"
    charm = KafkaCharm

    def get_harness(self) -> Tuple[Harness, int]:
        harness = Harness(self.charm, meta=self.metadata)
        # Set up the initial relation and hooks.
        rel_id = harness.add_relation(self.relation_name, "application")
        harness.add_relation_unit(rel_id, "application/0")

        # Juju 3 - specific setup
        self.setup_secrets_if_needed(harness, rel_id)

        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    @patch.object(KafkaCharm, "_on_topic_requested")
    def emit_topic_requested_event(self, _on_topic_requested):
        # Emit the topic requested event.
        relation = self.harness.charm.model.get_relation(self.relation_name, self.rel_id)
        application = self.harness.charm.model.get_app(self.app_name)
        self.harness.charm.provider.on.topic_requested.emit(relation, application)
        return _on_topic_requested.call_args[0][0]

    @patch.object(KafkaCharm, "_on_topic_requested")
    def test_on_topic_requested(self, _on_topic_requested):
        """Asserts that the correct hook is called when a new topic is requested."""
        # Simulate the request of a new topic plus extra user roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {"topic": TOPIC, "extra-user-roles": EXTRA_USER_ROLES},
        )

        # Assert the correct hook is called.
        _on_topic_requested.assert_called_once()

        # Assert the topic name and the extra user roles
        # are accessible in the providers charm library event.
        event = _on_topic_requested.call_args[0][0]
        assert event.topic == TOPIC
        assert event.extra_user_roles == EXTRA_USER_ROLES

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_set_bootstrap_server(self):
        """Asserts that the bootstrap-server are in the relation databag when they change."""
        # Set the bootstrap-server in the relation using the provides charm library.
        self.harness.charm.provider.set_bootstrap_server(self.rel_id, "host1:port,host2:port")

        # Check that the bootstrap-server is present in the relation.
        assert (
            self.harness.get_relation_data(self.rel_id, self.app_name)["endpoints"]
            == "host1:port,host2:port"
        )

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_bootstrap_server_secrets(self):
        """Asserts that the bootstrap-server are in the relation databag when they change."""
        # Set the bootstrap-server in the relation using the provides charm library.
        self.harness.charm.provider.set_bootstrap_server(self.rel_id, "host1:port,host2:port")

        # Check that the bootstrap-server is present in the relation.
        secret_uri = self.harness.get_relation_data(self.rel_id, self.app_name)["secret-endpoints"]
        secret = self.harness.charm.model.get_secret(id=secret_uri)
        assert secret.get_content() == {"endpoints": "host1:port,host2:port"}

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_set_additional_fields(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_tls(self.rel_id, "True")
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")
        self.harness.charm.provider.set_consumer_group_prefix(self.rel_id, "pr1,pr2")
        self.harness.charm.provider.set_zookeeper_uris(self.rel_id, "host1:port,host2:port")

        # Check that the additional fields are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, self.app_name) == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "tls": "True",
            "tls-ca": "Canonical",
            "zookeeper-uris": "host1:port,host2:port",
            "consumer-group-prefix": "pr1,pr2",
        }

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_additional_fields_secrets(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_tls(self.rel_id, "True")
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")
        self.harness.charm.provider.set_consumer_group_prefix(self.rel_id, "pr1,pr2")
        self.harness.charm.provider.set_zookeeper_uris(self.rel_id, "host1:port,host2:port")

        # Check that the additional fields are present in the relation.
        relation_data = self.harness.get_relation_data(self.rel_id, self.app_name)
        secret_id = relation_data.pop("secret-tls")
        assert secret_id

        relation_data == {
            "data": '{"secret_fields": "'
            + f"{self.SECRET_FIELDS}"
            + '"}',  # Data is the diff stored between multiple relation changed events.   # noqa
            "zookeeper-uris": "host1:port,host2:port",
            "consumer-group-prefix": "pr1,pr2",
        }

        secret = self.harness.charm.model.get_secret(id=secret_id)
        assert secret.get_content() == {
            "tls": "True",
            "tls-ca": "Canonical",
        }

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_fetch_relation_data(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"topic": TOPIC})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"topic": TOPIC}}

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_fetch_relation_data_secrets(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"topic": TOPIC})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"topic": TOPIC, "secret_fields": self.SECRET_FIELDS}}

    def test_topic_requested_event(self):
        # Test custom event creation

        # Test the event being emitted by the application.
        with capture(self.harness.charm, TopicRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application", {"topic": TOPIC})
        assert captured.event.app.name == "application"

        # Reset the diff data to trigger the event again later.
        self.harness.update_relation_data(self.rel_id, self.app_name, {"data": "{}"})

        # Test the event being emitted by the unit.
        with capture(self.harness.charm, TopicRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application/0", {"topic": TOPIC})
        assert captured.event.unit.name == "application/0"


class TestOpenSearchProvides(DataProvidesBaseTests, unittest.TestCase):
    metadata = OPENSEARCH_METADATA
    relation_name = OPENSEARCH_RELATION_NAME
    app_name = "opensearch"
    charm = OpenSearchCharm

    def get_harness(self) -> Tuple[Harness, int]:
        harness = Harness(self.charm, meta=self.metadata)
        # Set up the initial relation and hooks.
        rel_id = harness.add_relation(self.relation_name, "application")
        harness.add_relation_unit(rel_id, "application/0")

        # Juju 3 - specific setup
        self.setup_secrets_if_needed(harness, rel_id)

        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    @patch.object(OpenSearchCharm, "_on_index_requested")
    def emit_topic_requested_event(self, _on_index_requested):
        # Emit the topic requested event.
        relation = self.harness.charm.model.get_relation(self.relation_name, self.rel_id)
        application = self.harness.charm.model.get_app(self.app_name)
        self.harness.charm.provider.on.index_requested.emit(relation, application)
        return _on_index_requested.call_args[0][0]

    @patch.object(OpenSearchCharm, "_on_index_requested")
    def test_on_index_requested(self, _on_index_requested):
        """Asserts that the correct hook is called when a new topic is requested."""
        # Simulate the request of a new topic plus extra user roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {"index": INDEX, "extra-user-roles": EXTRA_USER_ROLES},
        )

        # Assert the correct hook is called.
        _on_index_requested.assert_called_once()

        # Assert the topic name and the extra user roles
        # are accessible in the providers charm library event.
        event = _on_index_requested.call_args[0][0]
        assert event.index == INDEX
        assert event.extra_user_roles == EXTRA_USER_ROLES

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_set_additional_fields(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")

        # Check that the additional fields are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, self.app_name) == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "tls-ca": "Canonical",
        }

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_set_additional_fields_secrets(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")

        # Check that the additional fields are present in the relation.
        relation_data = self.harness.get_relation_data(self.rel_id, self.app_name)
        secret_id = relation_data.pop("secret-tls")
        assert secret_id

        relation_data == {
            "data": '{"secret_fields": "'
            + f"{self.SECRET_FIELDS}"
            + '"}',  # Data is the diff stored between multiple relation changed events.   # noqa
        }

        secret = self.harness.charm.model.get_secret(id=secret_id)
        assert secret.get_content() == {
            "tls-ca": "Canonical",
        }

    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_fetch_relation_data(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"index": INDEX})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"index": INDEX}}

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_fetch_relation_data_secrets(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"index": INDEX})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"index": INDEX, "secret_fields": self.SECRET_FIELDS}}

    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_index_requested_event(self):
        # Test custom event creation

        # Test the event being emitted by the application.
        with capture(self.harness.charm, IndexRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application", {"index": INDEX})
        assert captured.event.app.name == "application"

        # Reset the diff data to trigger the event again later.
        self.harness.update_relation_data(self.rel_id, self.app_name, {"data": "{}"})

        # Test the event being emitted by the unit.
        with capture(self.harness.charm, IndexRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application/0", {"index": INDEX})
        assert captured.event.unit.name == "application/0"


CLUSTER_ALIASES = ["cluster1", "cluster2"]
DATABASE = "data_platform"
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"
DATABASE_RELATION_INTERFACE = "database_client"
DATABASE_RELATION_NAME = "database"
KAFKA_RELATION_INTERFACE = "kafka_client"
KAFKA_RELATION_NAME = "kafka"
METADATA = f"""
name: application
requires:
  {DATABASE_RELATION_NAME}:
    interface: {DATABASE_RELATION_INTERFACE}
    limit: {len(CLUSTER_ALIASES)}
  {KAFKA_RELATION_NAME}:
    interface: {KAFKA_RELATION_INTERFACE}
  {OPENSEARCH_RELATION_NAME}:
    interface: {OPENSEARCH_RELATION_INTERFACE}
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
        self.framework.observe(
            self.on[DATABASE_RELATION_NAME].relation_broken, self._on_relation_broken
        )
        self.framework.observe(self.requirer.on.endpoints_changed, self._on_endpoints_changed)
        self.framework.observe(
            self.requirer.on.read_only_endpoints_changed, self._on_read_only_endpoints_changed
        )
        self.framework.observe(
            self.requirer.on.cluster1_database_created, self._on_cluster1_database_created
        )

    def log_relation_size(self, prefix=""):
        logger.info(f"§{prefix} relations: {len(self.requirer.relations)}")

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

    def _on_database_created(self, _) -> None:
        self.log_relation_size("on_database_created")

    def _on_relation_broken(self, _) -> None:
        # This should not raise errors
        self.requirer.fetch_relation_data()

        self.log_relation_size("on_relation_broken")

    def _on_endpoints_changed(self, _) -> None:
        self.log_relation_size("on_endpoints_changed")

    def _on_read_only_endpoints_changed(self, _) -> None:
        self.log_relation_size("on_read_only_endpoints_changed")

    def _on_cluster1_database_created(self, _) -> None:
        self.log_relation_size("on_cluster1_database_created")


class ApplicationCharmKafka(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = KafkaRequires(self, KAFKA_RELATION_NAME, TOPIC, EXTRA_USER_ROLES)
        self.framework.observe(self.requirer.on.topic_created, self._on_topic_created)
        self.framework.observe(
            self.requirer.on.bootstrap_server_changed, self._on_bootstrap_server_changed
        )

    def _on_topic_created(self, _) -> None:
        pass

    def _on_bootstrap_server_changed(self, _) -> None:
        pass


class ApplicationCharmOpenSearch(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = OpenSearchRequires(self, OPENSEARCH_RELATION_NAME, INDEX, EXTRA_USER_ROLES)
        self.framework.observe(self.requirer.on.index_created, self._on_index_created)

    def _on_index_created(self, _) -> None:
        pass


@pytest.fixture(autouse=True)
def reset_aliases():
    """Fixture that runs before each test to delete the custom events created for the aliases.

    This is needed because the events are created again in the next test,
    which causes an error related to duplicated events.
    """
    for cluster_alias in CLUSTER_ALIASES:
        try:
            delattr(DatabaseRequiresEvents, f"{cluster_alias}_database_created")
            delattr(DatabaseRequiresEvents, f"{cluster_alias}_endpoints_changed")
            delattr(DatabaseRequiresEvents, f"{cluster_alias}_read_only_endpoints_changed")
        except AttributeError:
            # Ignore the events not existing before the first test.
            pass


class DataRequirerBaseTests(ABC):
    metadata: str
    relation_name: str
    app_name: str
    charm: Type[CharmBase]

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

        mock_event = Mock()
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app(self.app_name)
        local_unit = self.harness.charm.model.get_unit(f"{self.app_name}/0")
        mock_event.relation.id = rel_id
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


class TestDatabaseRequiresNoRelations(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = DATABASE_RELATION_NAME
    charm = ApplicationCharmDatabase

    app_name = "application"
    provider = "database"

    def setUp(self):
        self.harness = self.get_harness()
        self.harness.begin_with_initial_hooks()

    def test_empty_resource_created(self):
        self.assertFalse(self.harness.charm.requirer.is_resource_created())

    def test_non_existing_resource_created(self):
        self.assertRaises(IndexError, lambda: self.harness.charm.requirer.is_resource_created(0))
        self.assertRaises(IndexError, lambda: self.harness.charm.requirer.is_resource_created(1))

    def test_hide_relation_on_broken_event(self):
        with self.assertLogs(logger, "INFO") as logs:
            rel_id = self.add_relation(self.harness, self.provider)
            self.harness.update_relation_data(
                rel_id, self.provider, {"username": "username", "password": "password"}
            )

        # make sure two events were fired
        self.assertEqual(len(logs.output), 2)
        self.assertListEqual(
            [self.harness.charm.get_prefix(log) for log in logs.output],
            ["on_database_created", "on_cluster1_database_created"],
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

    def setUp(self):
        self.harness = self.get_harness()
        self.rel_id = self.add_relation(self.harness, self.provider)
        self.harness.begin_with_initial_hooks()

    @patch.object(charm, "_on_database_created")
    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_on_database_created(self, _on_database_created):
        """Asserts on_database_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created database.
        assert not self.harness.charm.requirer.is_resource_created()

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_database_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert isinstance(event, AuthenticationEvent)
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "username")
            == "test-username"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "password")
            == "test-password"
        )

        assert self.harness.charm.requirer.is_resource_created()

        rel_id = self.add_relation(self.harness, self.provider)

        assert not self.harness.charm.requirer.is_resource_created()
        assert not self.harness.charm.requirer.is_resource_created(rel_id)

        self.harness.update_relation_data(
            rel_id,
            self.provider,
            {"username": "test-username-2", "password": "test-password-2"},
        )

        # Assert the correct hook is called.
        assert _on_database_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert isinstance(event, AuthenticationEvent)
        assert (
            self.harness.charm.requirer.get_relation_field(rel_id, "username") == "test-username-2"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(rel_id, "password") == "test-password-2"
        )

        assert self.harness.charm.requirer.is_resource_created(rel_id)
        assert self.harness.charm.requirer.is_resource_created()

    @patch.object(charm, "_on_database_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_on_database_created_secrets(self, _on_database_created):
        """Asserts on_database_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created database.
        assert not self.harness.charm.requirer.is_resource_created()

        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )

        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})

        # Assert the correct hook is called.
        _on_database_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-user"] == secret.id

        assert self.harness.charm.requirer.is_resource_created()

        rel_id = self.add_relation(self.harness, self.provider)

        assert not self.harness.charm.requirer.is_resource_created()
        assert not self.harness.charm.requirer.is_resource_created(rel_id)

        secret2 = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )
        self.harness.update_relation_data(rel_id, self.provider, {"secret-user": secret2.id})

        # Assert the correct hook is called.
        assert _on_database_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-user"] == secret2.id

        assert self.harness.charm.requirer.is_resource_created(rel_id)
        assert self.harness.charm.requirer.is_resource_created()

    @patch.object(charm, "_on_database_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_on_database_created_requires_juju3_provides_juju2(self, _on_database_created):
        """Asserts that the databag is used if one side of the relation is on Juju2."""
        # Simulate sharing the credentials of a new created database.
        assert not self.harness.charm.requirer.is_resource_created()

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_database_created.assert_called_once()

        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "username")
            == "test-username"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "password")
            == "test-password"
        )

    @patch.object(charm, "_on_endpoints_changed")
    def test_on_endpoints_changed(self, _on_endpoints_changed):
        """Asserts the correct call to on_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
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
            self.provider,
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_endpoints_changed.assert_called_once()

    @patch.object(charm, "_on_endpoints_changed")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_on_endpoints_changed_secrets(self, _on_endpoints_changed):
        """Asserts the correct call to on_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        secret = self.harness.charm.app.add_secret({"endpoints": "host1:port,host2:port"})

        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints": secret.id}
        )
        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints-revision": "1"}
        )

        # Assert the correct hook is called.
        _on_endpoints_changed.assert_called_once()

        # Check that the endpoints are present in the relation
        # using the requires charm library event.
        event = _on_endpoints_changed.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-endpoints"] == secret.id

        # Reset the mock call count.
        _on_endpoints_changed.reset_mock()

        # NOTE: Unlike for Juju2 databag tests, here we can't really simulate
        # that there was an attempt to re-set the secret with equal contents

        # Then, change the endpoints in the relation.
        secret.set_content({"endpoints": "host1:port,host2:port,host3:port"})
        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints-revision": "2"}
        )

        _on_endpoints_changed.assert_called_once()

    @patch.object(charm, "_on_read_only_endpoints_changed")
    def test_on_read_only_endpoints_changed(self, _on_read_only_endpoints_changed):
        """Asserts the correct call to on_read_only_endpoints_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
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
            self.provider,
            {"read-only-endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_read_only_endpoints_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"read-only-endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_read_only_endpoints_changed.assert_called_once()

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
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
            self.provider,
            {
                "read-only-endpoints": "host1:port,host2:port",
                "replset": "rs0",
                "version": "1.0",
            },
        )

        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})
        secret = self.harness.charm.app.add_secret({"endpoints": "host1:port,host2:port"})
        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints": secret.id}
        )
        secret = self.harness.charm.app.add_secret({"tls": "True", "tls-ca": "Canonical"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-tls": secret.id})
        secret = self.harness.charm.app.add_secret({"uris": "host1:port,host2:port"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-uris": secret.id})

        # Check that the fields are present in the relation
        # using the requires charm library event.
        event = _on_database_created.call_args[0][0]
        assert event.read_only_endpoints == "host1:port,host2:port"
        assert event.replset == "rs0"
        assert event.version == "1.0"

    @patch.object(charm, "_on_database_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_fields_are_accessible_through_interface_functions(self, _on_database_created):
        """Asserts fields are accessible through the requires charm library event."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "read-only-endpoints": "host1:port,host2:port",
                "replset": "rs0",
                "version": "1.0",
            },
        )

        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})
        secret = self.harness.charm.app.add_secret({"endpoints": "host1:port,host2:port"})
        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints": secret.id}
        )
        secret = self.harness.charm.app.add_secret({"tls": "True", "tls-ca": "Canonical"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-tls": secret.id})
        secret = self.harness.charm.app.add_secret({"uris": "host1:port,host2:port"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-uris": secret.id})

        # Secret fields
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "username")
            == "test-username"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "password")
            == "test-password"
        )
        assert self.harness.charm.requirer.get_relation_fields(
            self.rel_id, ["endpoints", "tls", "tls-ca", "uris"]
        ) == {
            "endpoints": "host1:port,host2:port",
            "tls": "True",
            "tls-ca": "Canonical",
            "uris": "host1:port,host2:port",
        }

        # Databag fields
        assert self.harness.charm.requirer.get_relation_fields(
            self.rel_id, ["read-only-endpoints", "replset", "version"]
        ) == {"read-only-endpoints": "host1:port,host2:port", "replset": "rs0", "version": "1.0"}

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

    @patch.object(charm, "_on_cluster1_database_created")
    def test_emit_aliased_event(self, _on_cluster1_database_created):
        """Asserts the correct custom event is triggered."""
        # Reset the diff/data key in the relation to correctly emit the event.
        self.harness.update_relation_data(self.rel_id, self.app_name, {"data": "{}"})

        # Check that the event wasn't triggered yet.
        _on_cluster1_database_created.assert_not_called()

        # Call the emit function and assert the desired event is triggered.
        relation = self.harness.charm.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        mock_event = Mock()
        mock_event.app = self.harness.charm.model.get_app(self.app_name)
        mock_event.unit = self.harness.charm.model.get_unit(f"{self.app_name}/0")
        mock_event.relation = relation
        self.harness.charm.requirer._emit_aliased_event(mock_event, "database_created")
        _on_cluster1_database_created.assert_called_once()

    def test_get_relation_alias(self):
        """Asserts the correct relation alias is returned."""
        # Assert the relation got the first cluster alias.
        assert self.harness.charm.requirer._get_relation_alias(self.rel_id) == CLUSTER_ALIASES[0]

    @patch("psycopg.connect")
    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_is_postgresql_plugin_enabled(self, _connect):
        """Asserts that the function correctly returns whether a plugin is enabled."""
        plugin = "citext"

        # Assert False is returned when there is no endpoint available.
        assert not self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

        # Assert False when the connection to the database fails.
        _connect.side_effect = psycopg.Error
        with self.harness.hooks_disabled():
            self.harness.update_relation_data(
                self.rel_id, self.provider, {"endpoints": "test-endpoint:5432"}
            )
        assert not self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

        _connect.side_effect = None
        # Assert False when the plugin is disabled.
        connect_cursor = _connect.return_value.__enter__.return_value.cursor
        connect_cursor.return_value.__enter__.return_value.fetchone.return_value = None
        assert not self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

        # Assert True when the plugin is enabled.
        connect_cursor.return_value.__enter__.return_value.fetchone.return_value = True
        assert self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

    @patch("psycopg.connect")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_is_postgresql_plugin_enabled_secrets(self, _connect):
        """Asserts that the function correctly returns whether a plugin is enabled."""
        plugin = "citext"

        # Assert False is returned when there is no endpoint available.
        assert not self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

        secret = self.harness.charm.app.add_secret({"endpoints": "test-endpoint:5432"})

        # Assert False when the connection to the database fails.
        _connect.side_effect = psycopg.Error
        with self.harness.hooks_disabled():
            self.harness.update_relation_data(
                self.rel_id, self.provider, {"secret-endpoints": secret.id}
            )
        assert not self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

        _connect.side_effect = None
        # Assert False when the plugin is disabled.
        connect_cursor = _connect.return_value.__enter__.return_value.cursor
        connect_cursor.return_value.__enter__.return_value.fetchone.return_value = None
        assert not self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

        # Assert True when the plugin is enabled.
        connect_cursor.return_value.__enter__.return_value.fetchone.return_value = True
        assert self.harness.charm.requirer.is_postgresql_plugin_enabled(plugin)

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
            previous_event_diff = self.harness.get_relation_data(
                self.rel_id, f"{self.app_name}/0"
            ).get("data")

            # Test the event being emitted by the application.
            with capture_events(self.harness.charm, *all_events) as captured_events:
                self.harness.update_relation_data(self.rel_id, self.provider, event["data"])

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
                self.harness.update_relation_data(self.rel_id, f"{self.provider}/0", event["data"])

            # There are two events (one aliased and the other without alias).
            assert len(captured_events) == 2

            # Check that the events that were emitted are the ones that were expected.
            assert all(
                isinstance(captured_event, event["event"]) for captured_event in captured_events
            )

            # Test that the remote unit name is available in the event.
            for captured in captured_events:
                assert captured.unit.name == f"{self.provider}/0"


class TestKafkaRequires(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = KAFKA_RELATION_NAME
    charm = ApplicationCharmKafka

    app_name = "application"
    provider = "kafka"

    def setUp(self):
        self.harness = self.get_harness()
        self.rel_id = self.add_relation(self.harness, self.provider)
        self.harness.begin_with_initial_hooks()

    @patch.object(charm, "_on_topic_created")
    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_on_topic_created(
        self,
        _on_topic_created,
    ):
        """Asserts on_topic_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created topic.

        assert not self.harness.charm.requirer.is_resource_created()

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_topic_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert isinstance(event, AuthenticationEvent)
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "username")
            == "test-username"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "password")
            == "test-password"
        )

        assert self.harness.charm.requirer.is_resource_created()

        rel_id = self.add_relation(self.harness, self.provider)

        assert not self.harness.charm.requirer.is_resource_created()
        assert not self.harness.charm.requirer.is_resource_created(rel_id)

        self.harness.update_relation_data(
            rel_id,
            self.provider,
            {"username": "test-username-2", "password": "test-password-2"},
        )

        # Assert the correct hook is called.
        assert _on_topic_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert isinstance(event, AuthenticationEvent)
        assert (
            self.harness.charm.requirer.get_relation_field(rel_id, "username") == "test-username-2"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(rel_id, "password") == "test-password-2"
        )

        assert self.harness.charm.requirer.is_resource_created(rel_id)
        assert self.harness.charm.requirer.is_resource_created()

    @patch.object(charm, "_on_topic_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_on_topic_created_secret(
        self,
        _on_topic_created,
    ):
        """Asserts on_topic_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created topic.

        assert not self.harness.charm.requirer.is_resource_created()

        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )

        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})

        # Assert the correct hook is called.
        _on_topic_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-user"] == secret.id

        assert self.harness.charm.requirer.is_resource_created()

        rel_id = self.add_relation(self.harness, self.provider)

        assert not self.harness.charm.requirer.is_resource_created()
        assert not self.harness.charm.requirer.is_resource_created(rel_id)

        secret2 = self.harness.charm.app.add_secret(
            {"username": "test-username2", "password": "test-password2"}
        )

        self.harness.update_relation_data(rel_id, self.provider, {"secret-user": secret2.id})

        # Assert the correct hook is called.
        assert _on_topic_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-user"] == secret2.id

        assert self.harness.charm.requirer.is_resource_created(rel_id)
        assert self.harness.charm.requirer.is_resource_created()

    @patch.object(charm, "_on_bootstrap_server_changed")
    def test_on_bootstrap_server_changed(self, _on_bootstrap_server_changed):
        """Asserts the correct call to _on_bootstrap_server_changed."""
        # Simulate adding endpoints to the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
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
            self.provider,
            {"endpoints": "host1:port,host2:port"},
        )

        # Assert the hook was not called again.
        _on_bootstrap_server_changed.assert_not_called()

        # Then, change the endpoints in the relation.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"endpoints": "host1:port,host2:port,host3:port"},
        )

        # Assert the hook is called now.
        _on_bootstrap_server_changed.assert_called_once()

    def test_wildcard_topic(self):
        """Asserts Exception raised on wildcard being used for topic."""
        with self.assertRaises(ValueError):
            self.harness.charm.requirer.topic = WILDCARD_TOPIC

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
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
            self.provider,
            {
                "zookeeper-uris": "h1:port,h2:port",
                "consumer-group-prefix": "pr1,pr2",
            },
        )
        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})
        secret = self.harness.charm.app.add_secret({"endpoints": "host1:port,host2:port"})
        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints": secret.id}
        )
        secret = self.harness.charm.app.add_secret({"tls": "True", "tls-ca": "Canonical"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-tls": secret.id})
        secret = self.harness.charm.app.add_secret({"uris": "host1:port,host2:port"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-uris": secret.id})

        # Check that the fields are present in the relation
        # using the requires charm library event.
        event = _on_topic_created.call_args[0][0]
        assert event.zookeeper_uris == "h1:port,h2:port"
        assert event.consumer_group_prefix == "pr1,pr2"

    @patch.object(charm, "_on_topic_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_fields_are_accessible_through_interface_functions(self, _on_topic_created):
        """Asserts fields are accessible through the requires charm library event."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "zookeeper-uris": "h1:port,h2:port",
                "consumer-group-prefix": "pr1,pr2",
            },
        )
        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})
        secret = self.harness.charm.app.add_secret({"endpoints": "host1:port,host2:port"})
        self.harness.update_relation_data(
            self.rel_id, self.provider, {"secret-endpoints": secret.id}
        )
        secret = self.harness.charm.app.add_secret({"tls": "True", "tls-ca": "Canonical"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-tls": secret.id})
        secret = self.harness.charm.app.add_secret({"uris": "host1:port,host2:port"})
        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-uris": secret.id})

        # Secret fields
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "username")
            == "test-username"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "password")
            == "test-password"
        )
        assert self.harness.charm.requirer.get_relation_fields(
            self.rel_id, ["endpoints", "tls", "tls-ca", "uris"]
        ) == {
            "endpoints": "host1:port,host2:port",
            "tls": "True",
            "tls-ca": "Canonical",
            "uris": "host1:port,host2:port",
        }

        # Databag fields
        assert self.harness.charm.requirer.get_relation_fields(
            self.rel_id, ["zookeeper-uris", "consumer-group-prefix"]
        ) == {"zookeeper-uris": "h1:port,h2:port", "consumer-group-prefix": "pr1,pr2"}


class TestOpenSearchRequires(DataRequirerBaseTests, unittest.TestCase):
    metadata = METADATA
    relation_name = OPENSEARCH_RELATION_NAME
    charm = ApplicationCharmOpenSearch

    app_name = "application"
    provider = "opensearch"

    def setUp(self):
        self.harness = self.get_harness()
        self.rel_id = self.add_relation(self.harness, self.provider)
        self.harness.begin_with_initial_hooks()

    @patch.object(charm, "_on_index_created")
    @pytest.mark.usefixtures("only_without_juju_secrets")
    def test_on_index_created(
        self,
        _on_index_created,
    ):
        """Asserts on_index_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created topic.

        assert not self.harness.charm.requirer.is_resource_created()

        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {"username": "test-username", "password": "test-password"},
        )

        # Assert the correct hook is called.
        _on_index_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_index_created.call_args[0][0]
        assert isinstance(event, AuthenticationEvent)
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "username")
            == "test-username"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(self.rel_id, "password")
            == "test-password"
        )

        assert self.harness.charm.requirer.is_resource_created()

        rel_id = self.add_relation(self.harness, self.provider)
        assert not self.harness.charm.requirer.is_resource_created()
        assert not self.harness.charm.requirer.is_resource_created(rel_id)

        self.harness.update_relation_data(
            rel_id,
            self.provider,
            {"username": "test-username-2", "password": "test-password-2"},
        )

        # Assert the correct hook is called.
        assert _on_index_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_index_created.call_args[0][0]
        assert isinstance(event, AuthenticationEvent)
        assert (
            self.harness.charm.requirer.get_relation_field(rel_id, "username") == "test-username-2"
        )
        assert (
            self.harness.charm.requirer.get_relation_field(rel_id, "password") == "test-password-2"
        )

        assert self.harness.charm.requirer.is_resource_created(rel_id)
        assert self.harness.charm.requirer.is_resource_created()

    @patch.object(charm, "_on_index_created")
    @pytest.mark.usefixtures("only_with_juju_secrets")
    def test_on_index_created_secret(
        self,
        _on_index_created,
    ):
        """Asserts on_index_created is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created topic.

        assert not self.harness.charm.requirer.is_resource_created()

        secret = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )

        self.harness.update_relation_data(self.rel_id, self.provider, {"secret-user": secret.id})

        # Assert the correct hook is called.
        _on_index_created.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_index_created.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-user"] == secret.id

        assert self.harness.charm.requirer.is_resource_created()

        rel_id = self.add_relation(self.harness, self.provider)

        assert not self.harness.charm.requirer.is_resource_created()
        assert not self.harness.charm.requirer.is_resource_created(rel_id)

        secret2 = self.harness.charm.app.add_secret(
            {"username": "test-username", "password": "test-password"}
        )

        self.harness.update_relation_data(rel_id, self.provider, {"secret-user": secret2.id})

        # Assert the correct hook is called.
        assert _on_index_created.call_count == 2

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_index_created.call_args[0][0]
        assert event.relation.data[event.relation.app]["secret-user"] == secret2.id

        assert self.harness.charm.requirer.is_resource_created(rel_id)
        assert self.harness.charm.requirer.is_resource_created()

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            self.provider,
            {
                "tls-ca": "Canonical",
                "version": "1.0",
            },
        )

        # Check that the fields are present in the relation
        # using the requires charm library.
        relation_data = self.harness.charm.requirer.fetch_relation_data()[self.rel_id]
        assert relation_data["tls-ca"] == "Canonical"
        assert relation_data["version"] == "1.0"
