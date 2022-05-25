# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest

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
        self.harness.set_leader(True)
        self.harness.begin_with_initial_hooks()

    def test_database_is_requested(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        rel_id = self.harness.add_relation(RELATION_NAME, "database")
        self.harness.add_relation_unit(rel_id, "database/0")

        # Set the database name in the relation using the requires charm library.
        self.harness.charm.database.set_database(DATABASE)

        # Check that the database name is present in the relation.
        assert self.harness.get_relation_data(rel_id, "application") == {"database": DATABASE}
