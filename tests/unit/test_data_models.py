# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import unittest
from typing import List, Optional, Union
from unittest.mock import Mock

from charms.data_platform_libs.v0.data_models import (
    BaseConfigModel,
    RelationDataModel,
    TypedCharmBase,
    get_relation_data_as,
    parse_relation_data,
    validate_params,
)
from ops.charm import ActionEvent, RelationEvent
from ops.testing import Harness
from pydantic import BaseModel, ValidationError, validator

METADATA = """
    name: test-app
    requires:
      database:
        interface: database
"""

CONFIG = """
    options:
      float-config:
        default: "1.0"
        type: string
      low-value-config:
        default: 10
        type: int
"""

ACTIONS = """
    set-server:
      description: Set server.
      params:
        host:
          type: string
        port:
          type: int
      required: [host]
"""


class CharmConfig(BaseConfigModel):
    float_config: float
    low_value_config: int

    @validator("low_value_config")
    @classmethod
    def less_than_100(cls, value: int):
        if value >= 100:
            raise ValueError("Value too large")
        return value


class ActionModel(BaseModel):
    host: str
    port: int = 80


class NestedField(BaseModel):
    key: List[int]


class NestedDataBag(RelationDataModel):
    nested_field: NestedField


class ProviderDataBag(BaseModel):
    key: float


class MergedDataBag(NestedDataBag, ProviderDataBag):
    pass


logger = logging.getLogger(__name__)


class TestCharmCharm(TypedCharmBase[CharmConfig]):
    """Mock database charm to use in units tests."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(getattr(self.on, "set_server_action"), self._set_server_action)
        self.framework.observe(
            self.on["database"].relation_joined, self._on_database_relation_joined
        )
        self.framework.observe(
            self.on["database"].relation_changed, self._on_database_relation_changed
        )

    @validate_params(ActionModel)
    def _set_server_action(
        self, event: ActionEvent, params: Optional[Union[ActionModel, ValidationError]] = None
    ):
        if isinstance(params, ValidationError):
            event.fail("Validation failed")
            logger.error(params)
            return False
        payload = {"server": f"{params.host}:{params.port}"}
        logger.info(payload["server"])
        event.set_results(payload)
        return True

    def _on_database_relation_joined(self, event: RelationEvent):
        relation_data = event.relation.data[self.app]
        data = NestedDataBag(nested_field=NestedField(key=[1, 2, 3]))
        data.write(relation_data)

    @parse_relation_data(app_model=ProviderDataBag)
    def _on_database_relation_changed(
        self,
        event: RelationEvent,
        app_data: Optional[Union[ProviderDataBag, ValidationError]] = None,
        _=None,
    ):
        logger.info(type(app_data.key))


class TestCharm(unittest.TestCase):
    harness = Harness(TestCharmCharm, meta=METADATA, config=CONFIG, actions=ACTIONS)

    @classmethod
    def setUpClass(cls) -> None:
        cls.harness.set_model_name("testing")
        cls.harness.begin()

    def setUp(self) -> None:
        # Instantiate the Charmed Operator Framework test harness

        self.addCleanup(self.harness.cleanup)

        self.assertIsInstance(self.harness.charm, TestCharmCharm)

    def test_config_parsing_ok(self):
        self.assertIsInstance(self.harness.charm.config, CharmConfig)

        self.assertIsInstance(self.harness.charm.config.float_config, float)

        self.harness.update_config({"low-value-config": 1})
        self.assertEqual(self.harness.charm.config["low-value-config"], 1)

    def test_config_parsing_ko(self):
        self.harness.update_config({"low-value-config": 200})

        self.assertRaises(ValueError, lambda: self.harness.charm.config)

        self.harness.update_config({"low-value-config": 10})

    def test_action_params_parsing_ok(self):
        mock_event = Mock()
        mock_event.params = {"host": "my-host"}
        with self.assertLogs(level="INFO") as logger:
            self.assertTrue(self.harness.charm._set_server_action(mock_event))
        self.assertEqual(sorted(logger.output), ["INFO:test_data_models:my-host:80"])

    def test_action_params_parsing_ko(self):
        mock_event = Mock()
        mock_event.params = {"port": 8080}
        with self.assertLogs(level="ERROR") as logger:
            self.assertFalse(self.harness.charm._set_server_action(mock_event))
        self.assertTrue("validation error" in logger.output[0])
        self.assertTrue("field required" in logger.output[0])

    def test_relation_databag_io(self):
        relation_id = self.harness.add_relation("database", "mongodb")
        self.harness.set_leader(True)
        self.harness.add_relation_unit(relation_id, "mongodb/0")

        relation = self.harness.charm.model.get_relation("database", relation_id)

        my_data = NestedDataBag.read(relation.data[self.harness.charm.app])

        self.assertEqual(my_data.nested_field.key, [1, 2, 3])

        with self.assertLogs(level="INFO") as logger:
            self.harness.update_relation_data(relation_id, "mongodb", {"key": "1.0"})
        self.assertEqual(logger.output, ["INFO:test_data_models:<class 'float'>"])

    def test_relation_databag_merged(self):
        relation = self.harness.charm.model.get_relation("database")

        relation_data = relation.data

        merged_obj = get_relation_data_as(
            MergedDataBag,
            relation_data[self.harness.charm.app],
            relation_data[relation.app],
        )

        self.assertIsInstance(merged_obj, MergedDataBag)
        self.assertEqual(merged_obj.key, 1.0)
        self.assertEqual(merged_obj.nested_field.key, [1, 2, 3])
