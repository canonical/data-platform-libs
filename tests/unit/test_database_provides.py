# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import unittest
from unittest.mock import patch

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

    def _on_database_requested(self, _) -> None:
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

    @patch.object(DatabaseCharm, "_on_database_requested")
    def test_on_database_requested_is_called(self, _on_database_requested):
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
        ) == {
            "username": "test-username",
            "password": "test-password",
        }
