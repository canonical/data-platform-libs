# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import unittest
from unittest.mock import patch

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

    def test_set_database(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        # Set the database name in the relation using the requires charm library.
        self.harness.charm.database.set_database(DATABASE)

        # Check that the database name is present in the relation.
        assert self.harness.get_relation_data(self.rel_id, "application")["database"] == DATABASE

    @patch.object(ApplicationCharm, "_on_database_created")
    def test_on_database_created_is_called(self, _on_database_created):
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
