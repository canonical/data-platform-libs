# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest

from ops.testing import Harness

from charm import DataPlatformLibsCharm


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(DataPlatformLibsCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_charm_library(self):
        pass
