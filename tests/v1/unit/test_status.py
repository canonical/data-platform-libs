import json
from typing import Optional

import pytest
from ops.charm import CharmBase
from ops.testing import Harness

from charms.data_platform_libs.v1.data_interfaces import (
    STATUS_FIELD,
    RelationStatus,
    RelationStatusDict,
    RequirerCommonModel,
    ResourceProviderEventHandler,
    ResourceProviderModel,
    ResourceRequirerEventHandler,
    StatusRaisedEvent,
    StatusResolvedEvent,
)

RELATION_NAME = "data"
RESOURCE_NAME = "test"
INTERFACES = ("database_client", "kafka_client")


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

        def __init__(self, *args):
            super().__init__(*args)

            self.provider = ResourceProviderEventHandler(self, RELATION_NAME, RequirerCommonModel)

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

        def __init__(self, *args):
            super().__init__(*args)
            self.requirer = ResourceRequirerEventHandler(
                charm=self,
                relation_name=RELATION_NAME,
                requests=[RequirerCommonModel(resource=RESOURCE_NAME)],
                response_model=ResourceProviderModel,
            )
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


REQUEST_V1_DATA = {
    "version": "v1",
    "requests": json.dumps(
        [
            {
                "resource": "testdb",
                "salt": "kkkkkkkk",
                "request-id": "c759221a6c14c72a",
            }
        ]
    ),
}


def _get_statuses_from_relation_data(
    harness: Harness, relation_id: int, provider_app: Optional[str] = None
) -> list[RelationStatusDict]:
    _app_name = provider_app if provider_app else harness.charm.app.name
    data = harness.get_relation_data(relation_id, _app_name)
    return json.loads(data.get(STATUS_FIELD, "[]"))


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("harness", list(INTERFACES), indirect=True)
def test_raise_status_and_its_overloads(harness):
    rel_id = harness.add_relation(RELATION_NAME, "requirer", app_data=REQUEST_V1_DATA)
    # harness.add_relation_unit(rel_id, "requirer/0")
    msg_dict = {
        "code": 1000,
        "message": "test informational message",
        "resolution": "no action required",
    }
    harness.charm.provider.raise_status(rel_id, msg_dict)

    status_json = _get_statuses_from_relation_data(harness, rel_id)

    assert len(status_json) == 1
    for k, v in msg_dict.items():
        assert status_json[0][k] == v

    harness.charm.provider.raise_status(
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
        harness.charm.provider.raise_status(rel_id, 1002)

    with pytest.raises(ValueError):
        harness.charm.provider.raise_status(rel_id, '{{"code": 4000}}')

    status_json = _get_statuses_from_relation_data(harness, rel_id)
    assert len(status_json) == 2


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("harness", list(INTERFACES), indirect=True)
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
    rel_id = harness.add_relation(RELATION_NAME, "requirer", app_data=REQUEST_V1_DATA)
    for _status in statuses:
        harness.charm.provider.raise_status(rel_id, _status)

    harness.charm.provider.resolve_status(rel_id, 4000)
    status_json = _get_statuses_from_relation_data(harness, rel_id)

    assert len(status_json) == 2
    assert {s["code"] for s in status_json} == {1002, 4001}

    # Arrange
    harness.charm.provider.raise_status(
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

    harness.charm.provider.clear_statuses(rel_id)
    status_json = _get_statuses_from_relation_data(harness, rel_id)
    assert len(status_json) == 0


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("requirer_harness", list(INTERFACES), indirect=True)
def test_status_events_in_requirer(requirer_harness):
    _ = requirer_harness.add_relation(
        RELATION_NAME,
        "provider",
        app_data=REQUEST_V1_DATA
        | {
            STATUS_FIELD: '[{"code":1003,"message":"test","resolution":"nothing"}]',
            "data": '{"c759221a6c14c72a":{}}',
        },
    )
    assert len(requirer_harness.charm.statuses) == 1
    assert requirer_harness.charm.last_raised.code == 1003
    assert requirer_harness.charm.last_raised.message == "test"
    assert requirer_harness.charm.last_raised.resolution == "nothing"
