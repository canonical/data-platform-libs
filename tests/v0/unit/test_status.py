import json
from collections import namedtuple
from typing import Optional

import pytest
from ops.charm import CharmBase
from ops.testing import Harness

from charms.data_platform_libs.v0.data_interfaces import (
    STATUS_FIELD,
    DatabaseProviderData,
    DatabaseProviderEventHandlers,
    DatabaseRequires,
    EtcdProviderData,
    EtcdProviderEventHandlers,
    EtcdRequires,
    KafkaProviderData,
    KafkaProviderEventHandlers,
    KafkaRequires,
    OpenSearchProvidesData,
    OpenSearchProvidesEventHandlers,
    OpenSearchRequires,
    RelationStatus,
    RelationStatusDict,
    StatusRaisedEvent,
    StatusResolvedEvent,
)

RELATION_NAME = "data"
RESOURCE_NAME = "test"


interfaces_tuple = namedtuple("DataInterfaces", "data events resource")

PROVIDER_INTERFACES = {
    "database_client": interfaces_tuple(
        data=DatabaseProviderData, events=DatabaseProviderEventHandlers, resource="database"
    ),
    "kafka_client": interfaces_tuple(
        data=KafkaProviderData, events=KafkaProviderEventHandlers, resource="topic"
    ),
    "opensearch_client": interfaces_tuple(
        data=OpenSearchProvidesData, events=OpenSearchProvidesEventHandlers, resource="index"
    ),
    "etcd_client": interfaces_tuple(
        data=EtcdProviderData, events=EtcdProviderEventHandlers, resource="prefix"
    ),
}


REQUIRER_INTERFACES = {
    "database_client": DatabaseRequires,
    "kafka_client": KafkaRequires,
    "opensearch_client": OpenSearchRequires,
    "etcd_client": EtcdRequires,
}


@pytest.fixture
def harness(request: pytest.FixtureRequest) -> Harness:
    interface = request.param

    _metadata = f"""
    name: data-provider
    provides:
        {RELATION_NAME}:
            interface: {interface}
    """

    class _ProviderCharm(CharmBase):

        resource_type = PROVIDER_INTERFACES[interface].resource
        provider_data_cls = PROVIDER_INTERFACES[interface].data
        provider_events_cls = PROVIDER_INTERFACES[interface].events

        def __init__(self, *args):
            super().__init__(*args)

            self.provider_data = self.provider_data_cls(self.model, RELATION_NAME)
            self.provider_events = self.provider_events_cls(self, self.provider_data)

    harness = Harness(_ProviderCharm, meta=_metadata)
    harness.set_leader(True)
    harness.begin_with_initial_hooks()
    return harness


@pytest.fixture
def requirer_harness(request: pytest.FixtureRequest) -> Harness:
    interface = request.param

    _metadata = f"""
    name: data-requirer
    requires:
        {RELATION_NAME}:
            interface: {interface}
    """

    class _RequirerCharm(CharmBase):
        """Mock requirer charm to use in unit tests."""

        requirer_cls = REQUIRER_INTERFACES[interface]

        def __init__(self, *args):
            super().__init__(*args)
            extra_req_args = (None,) if interface == "etcd_client" else ()
            self.requirer = self.requirer_cls(self, RELATION_NAME, RESOURCE_NAME, *extra_req_args)
            self.framework.observe(self.requirer.on.status_raised, self._on_status_raised)
            self.framework.observe(self.requirer.on.status_resolved, self._on_status_resolved)
            self.last_raised = None
            self.last_resolved = None
            self.statuses = []

        def _on_status_raised(self, event: StatusRaisedEvent) -> None:
            self.statuses = event.active_statuses
            self.last_raised = event.status

        def _on_status_resolved(self, event: StatusResolvedEvent) -> None:
            self.statuses = event.active_statuses
            self.last_resolved = event.status

    harness = Harness(_RequirerCharm, meta=_metadata)
    harness.set_leader(True)
    harness.begin_with_initial_hooks()
    return harness


def _get_statuses_from_relation_data(
    harness: Harness, relation_id: int, provider_app: Optional[str] = None
) -> list[RelationStatusDict]:
    _app_name = provider_app if provider_app else harness.charm.app.name
    data = harness.get_relation_data(relation_id, _app_name)
    return json.loads(data.get(STATUS_FIELD, "[]"))


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("harness", list(PROVIDER_INTERFACES), indirect=True)
def test_raise_status_and_its_overloads(harness):
    rel_id = harness.add_relation(
        RELATION_NAME, "requirer", app_data={harness.charm.resource_type: RESOURCE_NAME}
    )
    # harness.add_relation_unit(rel_id, "requirer/0")
    msg_dict = {
        "code": 1000,
        "message": "test informational message",
        "resolution": "no action required",
    }
    harness.charm.provider_data.raise_status(rel_id, msg_dict)

    status_json = _get_statuses_from_relation_data(harness, rel_id)

    assert len(status_json) == 1
    for k, v in msg_dict.items():
        assert status_json[0][k] == v

    harness.charm.provider_data.raise_status(
        rel_id,
        RelationStatus(
            code=1001,
            message="another test information message",
            resolution="no action required, again",
        ),
    )

    status_json = _get_statuses_from_relation_data(harness, rel_id)

    assert len(status_json) == 2
    for _status in status_json:
        if _status.get("code") == 1001:
            assert _status.get("message") == "another test information message"
            assert _status.get("resolution") == "no action required, again"

    with pytest.raises(KeyError):
        # since no status-schema is defined
        harness.charm.provider_data.raise_status(rel_id, 1002)

    with pytest.raises(ValueError):
        harness.charm.provider_data.raise_status(rel_id, '{{"code": 4000}}')

    status_json = _get_statuses_from_relation_data(harness, rel_id)
    assert len(status_json) == 2


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("harness", list(PROVIDER_INTERFACES), indirect=True)
def test_resolve_and_clear_status(harness):
    statuses = [
        {
            "code": 1002,
            "message": "Test informational message.",
            "resolution": "No action required.",
        },
        {
            "code": 4000,
            "message": "Test transiorty issue of type 1.",
            "resolution": "Wait for a couple of minutes.",
        },
        {
            "code": 4001,
            "message": "Test transiorty issue of type 2.",
            "resolution": "Wait for a couple of hours.",
        },
    ]
    rel_id = harness.add_relation(
        RELATION_NAME, "requirer", app_data={harness.charm.resource_type: RESOURCE_NAME}
    )
    for _status in statuses:
        harness.charm.provider_data.raise_status(rel_id, _status)

    harness.charm.provider_data.resolve_status(rel_id, 4000)
    status_json = _get_statuses_from_relation_data(harness, rel_id)

    assert len(status_json) == 2
    assert {s["code"] for s in status_json} == {1002, 4001}

    # Arrange
    harness.charm.provider_data.raise_status(
        rel_id,
        RelationStatus(
            code=1001,
            message="another test information message",
            resolution="no action required, again",
        ),
    )
    status_json = _get_statuses_from_relation_data(harness, rel_id)
    assert len(status_json) == 3
    assert {s["code"] for s in status_json} == {1001, 1002, 4001}

    harness.charm.provider_data.clear_statuses(rel_id)
    status_json = _get_statuses_from_relation_data(harness, rel_id)
    assert len(status_json) == 0


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("requirer_harness", list(REQUIRER_INTERFACES), indirect=True)
def test_status_events_in_requirer(requirer_harness):
    rel_id = requirer_harness.add_relation(
        RELATION_NAME,
        "provider",
        app_data={STATUS_FIELD: '[{"code":1003,"message":"test","resolution":"nothing"}]'},
    )
    assert len(requirer_harness.charm.statuses) == 1
    assert requirer_harness.charm.last_raised.code == 1003
    assert requirer_harness.charm.last_raised.message == "test"
    assert requirer_harness.charm.last_raised.resolution == "nothing"

    requirer_harness.update_relation_data(
        rel_id,
        "provider",
        {
            STATUS_FIELD: '[{"code":1003,"message":"test","resolution":"nothing"},'
            '{"code":4001,"message":"temporary error","resolution":"wait"}]'
        },
    )

    assert len(requirer_harness.charm.statuses) == 2
    assert requirer_harness.charm.last_raised.code == 4001
    assert requirer_harness.charm.last_raised.message == "temporary error"
    assert requirer_harness.charm.last_raised.resolution == "wait"

    requirer_harness.update_relation_data(
        rel_id,
        "provider",
        {STATUS_FIELD: '[{"code":1003,"message":"test","resolution":"nothing"}]'},
    )

    assert len(requirer_harness.charm.statuses) == 1
    assert requirer_harness.charm.last_resolved.code == 4001
