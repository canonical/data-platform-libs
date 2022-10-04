# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import unittest
from unittest.mock import Mock, patch

from charms.data_platform_libs.v0.s3 import (
    CredentialRequestedEvent,
    Diff,
    S3Provider,
    S3Requirer,
)
from charms.harness_extensions.v0.capture_events import capture
from ops.charm import CharmBase
from ops.testing import Harness

RELATION_INTERFACE = "s3-credentials"
BUCKET_NAME = "test-bucket"
RELATION_NAME = "s3-credentials"

METADATA_APPLICATION = f"""
name: application
requires:
  {RELATION_NAME}:
    interface: {RELATION_INTERFACE}
"""

METADATA_S3 = f"""
name: s3_app
requires:
  {RELATION_NAME}:
    interface: {RELATION_INTERFACE}
"""

logger = logging.getLogger(__name__)


class ApplicationCharm(CharmBase):
    """Mock application charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.s3_requirer = S3Requirer(self, RELATION_NAME, BUCKET_NAME)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_credential_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_credential_gone)

    def _on_credential_changed(self, _) -> None:
        pass

    def _on_credential_gone(self, _) -> None:
        pass


class TestS3Requirer(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = Harness(ApplicationCharm, meta=METADATA_APPLICATION)
        self.addClassCleanup(self.harness.cleanup)

        # Set up the initial relation and hooks.
        self.rel_id = self.harness.add_relation(RELATION_NAME, "s3_app")
        self.harness.add_relation_unit(self.rel_id, "s3_app/0")
        self.harness.set_leader(True)
        self.harness.begin_with_initial_hooks()

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        mock_event = Mock()
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app("s3_app")
        local_unit = self.harness.charm.model.get_unit("application/0")
        mock_event.relation.id = self.rel_id
        mock_event.relation.data = {
            mock_event.app: {"access-key": "test-access-key", "secret-key": "test-secret-key"},
            local_unit: {},  # Initial empty databag in the local unit.
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.s3_requirer._diff(mock_event)
        assert result == Diff({"access-key", "secret-key"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.s3_requirer._diff(mock_event)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data["access-key"] = "new-test-access-key"
        result = self.harness.charm.s3_requirer._diff(mock_event)
        assert result == Diff(set(), {"access-key"}, set())

        # Test with deleted data.
        del data["access-key"]
        del data["secret-key"]
        result = self.harness.charm.s3_requirer._diff(mock_event)
        assert result == Diff(set(), set(), {"access-key", "secret-key"})

    @patch.object(ApplicationCharm, "_on_credential_changed")
    def test_on_credential_changed(self, _on_credential_changed):
        """Asserts on_credential_changed is called when the credentials are set in the relation."""
        # Simulate sharing the credentials of a new created relation.
        self.harness.update_relation_data(
            self.rel_id,
            "s3_app",
            {"access-key": "test-access-key", "secret-key": "test-secret-key"},
        )

        # Assert the correct hook is called.
        _on_credential_changed.assert_called_once()

        # Check that the username and the password are present in the relation
        # using the requires charm library event.
        event = _on_credential_changed.call_args[0][0]
        assert event.access_key == "test-access-key"
        assert event.secret_key == "test-secret-key"

    def test_additional_fields_are_accessible(self):
        """Asserts additional fields are accessible using the charm library after being set."""
        # Simulate setting the additional fields.
        self.harness.update_relation_data(
            self.rel_id,
            "s3_app",
            {
                "access-key": "test-access-key",
                "secret-key": "test-secret-key",
                "bucket": "test-bucket",
                "path": "/path/",
                "endpoint": "s3.amazonaws.com",
                "region": "us",
                "s3-uri-style": "",
                "storage-class": "cinder",
                "tls-ca-chain": "[]",
                "s3-api-version": "1.0",
                "attributes": '["a1", "a2", "a3"]',
            },
        )

        # Check that the fields are present in the relation
        # using the requires charm library.
        logger.info(f"Credential info: {self.harness.charm.s3_requirer.get_s3_connection_info()}")
        connection_info = self.harness.charm.s3_requirer.get_s3_connection_info()

        assert connection_info["bucket"] == "test-bucket"
        assert connection_info["path"] == "/path/"
        assert connection_info["endpoint"] == "s3.amazonaws.com"
        assert connection_info["region"] == "us"
        assert "s3-uri-style" not in connection_info
        assert connection_info["storage-class"] == "cinder"
        assert connection_info["tls-ca-chain"] == []
        assert connection_info["s3-api-version"] == 1.0
        assert connection_info["attributes"] == ["a1", "a2", "a3"]


class S3Charm(CharmBase):
    """Mock S3 charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.s3_provider = S3Provider(
            self,
            RELATION_NAME,
        )
        self.framework.observe(
            self.s3_provider.on.credentials_requested, self._on_credential_requested
        )

    def _on_credential_requested(self, _) -> None:
        pass


class TestS3Provider(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(S3Charm, meta=METADATA_S3)
        self.addCleanup(self.harness.cleanup)

        # Set up the initial relation and hooks.
        self.rel_id = self.harness.add_relation(RELATION_NAME, "application")
        self.harness.add_relation_unit(self.rel_id, "application/0")
        self.harness.set_leader(True)
        self.harness.begin_with_initial_hooks()

    @patch.object(S3Charm, "_on_credential_requested")
    def emit_credential_requested_event(self, _on_credential_requested):
        # Emit the credential requested event.
        relation = self.harness.charm.model.get_relation(RELATION_NAME, self.rel_id)
        application = self.harness.charm.model.get_app("s3_app")
        self.harness.charm.s3_provider.on.credendial_requested.emit(relation, application)
        return _on_credential_requested.call_args[0][0]

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        mock_event = Mock()
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app("s3_app")
        mock_event.relation.id = self.rel_id
        mock_event.relation.data = {
            mock_event.app: {"access-key": "test-access-key", "secret-key": "test-secret-key"}
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.s3_provider._diff(mock_event)
        assert result == Diff({"access-key", "secret-key"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.s3_provider._diff(mock_event)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data["access-key"] = "test-access-key-1"
        result = self.harness.charm.s3_provider._diff(mock_event)
        assert result == Diff(set(), {"access-key"}, set())

        # Test with deleted data.
        del data["access-key"]
        del data["secret-key"]
        result = self.harness.charm.s3_provider._diff(mock_event)
        assert result == Diff(set(), set(), {"access-key", "secret-key"})

    @patch.object(S3Charm, "_on_credential_requested")
    def test_on_credential_requested(self, _on_credential_requested):
        """Asserts that the correct hook is called when credential information is requested."""
        # Simulate the request of S3 credential infos.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {"bucket": BUCKET_NAME},
        )

        # Assert the correct hook is called.
        _on_credential_requested.assert_called_once()

        # Assert the bucket name are accessible in the providers charm library event.
        event = _on_credential_requested.call_args[0][0]
        assert event.bucket == BUCKET_NAME

    def test_set_connection_info(self):
        # TODO
        """Asserts that the s3 connection fields are in the relation databag when they are set."""
        # Set the connection info fields in the relation using the provides charm library.
        # Mandatory fields
        self.harness.charm.s3_provider.update_connection_info(
            self.rel_id, {"access-key": "test-access-key", "secret-key": "test-secret-key"}
        )
        logger.info(f"relation data 1: {self.harness.get_relation_data(self.rel_id, 's3_app')}")
        # Add extra fields
        self.harness.charm.s3_provider.set_access_key(self.rel_id, "test-access-key")
        self.harness.charm.s3_provider.set_secret_key(self.rel_id, "test-secret-key")
        self.harness.charm.s3_provider.set_bucket(self.rel_id, "test-bucket")
        self.harness.charm.s3_provider.set_path(self.rel_id, "/path/")
        self.harness.charm.s3_provider.set_endpoint(self.rel_id, "s3.amazonaws.com")
        self.harness.charm.s3_provider.set_region(self.rel_id, "us")
        self.harness.charm.s3_provider.set_s3_uri_style(self.rel_id, "style")
        self.harness.charm.s3_provider.set_storage_class(self.rel_id, "cinder")
        self.harness.charm.s3_provider.set_tls_ca_chain(self.rel_id, [])
        self.harness.charm.s3_provider.set_s3_api_version(self.rel_id, "1.0")
        self.harness.charm.s3_provider.set_attributes(self.rel_id, ["a1", "a2", "a3"])

        # Check that the additional fields are present in the relation.
        logger.info(f"relation data 2: {self.harness.get_relation_data(self.rel_id, 's3_app')}")
        assert self.harness.get_relation_data(self.rel_id, "s3_app") == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "access-key": "test-access-key",
            "secret-key": "test-secret-key",
            "bucket": "test-bucket",
            "path": "/path/",
            "endpoint": "s3.amazonaws.com",
            "region": "us",
            "s3-uri-style": "style",
            "storage-class": "cinder",
            "tls-ca-chain": "[]",
            "s3-api-version": "1.0",
            "attributes": '["a1", "a2", "a3"]',
        }

    def test_fetch_relation_data(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"bucket": BUCKET_NAME})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.s3_provider.fetch_relation_data()
        logger.info(f"Data: {data}")
        assert data == {self.rel_id: {"bucket": BUCKET_NAME}}

    def test_credential_requested_event(self):
        # Test custom event creation

        # Test the event being emitted by the application.
        with capture(self.harness.charm, CredentialRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application", {"bucket": BUCKET_NAME})
        assert captured.event.app.name == "application"

        # Reset the diff data to trigger the event again later.
        self.harness.update_relation_data(self.rel_id, "s3_app", {"data": "{}"})

        # Test the event being emitted by the unit.
        with capture(self.harness.charm, CredentialRequestedEvent) as captured:
            self.harness.update_relation_data(
                self.rel_id, "application/0", {"bucket": BUCKET_NAME}
            )
        assert captured.event.unit.name == "application/0"
