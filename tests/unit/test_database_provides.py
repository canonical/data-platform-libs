# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
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
        self.harness.set_leader(True)
        self.harness.begin_with_initial_hooks()

    @patch.object(DatabaseCharm, "_on_database_requested")
    def test_database_is_requested(self, _on_database_requested):
        """Asserts that the correct hook is called when a new database is requested."""
        rel_id = self.harness.add_relation(RELATION_NAME, "application")
        self.harness.add_relation_unit(rel_id, "application/0")

        self.harness.update_relation_data(rel_id, "application", {"database": DATABASE})

        # Assert the database name is accessible in the providers charm library.
        assert self.harness.charm.database.database == DATABASE

        # Assert the correct hook is called.
        _on_database_requested.assert_called_once()
