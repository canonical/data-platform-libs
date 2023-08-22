# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import unittest
from typing import List, Optional, Union
from unittest.mock import Mock

from ops.charm import ActionEvent, RelationEvent
from ops.testing import Harness
from parameterized import parameterized
from pydantic import BaseModel, ValidationError, validator

from charms.data_platform_libs.v0.data_models import (
    BaseConfigModel,
    RelationDataModel,
    TypedCharmBase,
    get_relation_data_as,
    parse_relation_data,
    validate_params,
    write,
)

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
    option_float: Optional[float] = None
    option_int: Optional[int] = None
    option_str: Optional[str] = None


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
        if isinstance(app_data, ProviderDataBag):
            logger.info("Field type: %s", type(app_data.key))
        elif isinstance(app_data, ValidationError):
            logger.info("Exception: %s", type(app_data))


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
        self.assertEqual(sorted(logger.output), ["INFO:unit.test_data_models:my-host:80"])

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
        self.assertEqual(logger.output, ["INFO:unit.test_data_models:Field type: <class 'float'>"])

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
        self.assertIsNone(merged_obj.option_float)
        self.assertIsNone(merged_obj.option_int)
        self.assertIsNone(merged_obj.option_str)

    @parameterized.expand(
        [
            ("option-float", "1.0", float),
            ("option-float", "1", float),
            ("option-int", "1", int),
            ("option-str", "1", str),
            ("option-str", "test", str),
        ]
    )
    def test_relation_databag_merged_with_option(self, option_key, option_value, _type):
        relation = self.harness.charm.model.get_relation("database")

        self.harness.update_relation_data(
            relation.id, "mongodb", {"key": "1.0", option_key: option_value}
        )

        relation_data = relation.data

        merged_obj = get_relation_data_as(
            MergedDataBag,
            relation_data[self.harness.charm.app],
            relation_data[relation.app],
        )

        self.assertIsNotNone(getattr(merged_obj, option_key.replace("-", "_")))
        self.assertIsInstance(getattr(merged_obj, option_key.replace("-", "_")), _type)

    @parameterized.expand(
        [
            (ProviderDataBag(key=1.0, option_float=2.0), "option-float", "2.0"),
            (ProviderDataBag(key=1.0, option_int=2.0), "option-int", "2"),
            (ProviderDataBag(key=1.0, option_str="test"), "option-str", "test"),
        ]
    )
    def test_relation_databag_write_with_option(self, databag, expected_key, expected_value):
        relation = self.harness.charm.model.get_relation("database")

        relation_data = relation.data[relation.app]

        write(relation_data, databag)

        self.assertIn(expected_key, relation_data)
        self.assertEqual(expected_value, relation_data[expected_key])

    def test_databag_parse_with_exception(self):
        merged_obj = get_relation_data_as(
            ProviderDataBag,
            {"key": "test"},
        )
        self.assertIsInstance(merged_obj, ValidationError)

    def test_relation_databag_parse_with_exception(self):
        relation = self.harness.charm.model.get_relation("database")

        with self.assertLogs(level="INFO") as logger:
            self.harness.update_relation_data(relation.id, "mongodb", {"key": "test"})
        self.assertTrue("Exception" in logger.output[0])
