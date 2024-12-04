# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging

import pytest
from ops.charm import CharmBase
from ops.model import BlockedStatus
from ops.testing import Harness
from pydantic import ValidationError

from charms.data_platform_libs.v0.upgrade import (
    BaseModel,
    DataUpgrade,
    DependencyModel,
    KubernetesClientError,
    VersionError,
    verify_requirements,
)

logger = logging.getLogger(__name__)

GANDALF_METADATA = """
name: gandalf
peers:
  upgrade:
    interface: upgrade
"""

GANDALF_ACTIONS = """
pre-upgrade-check:
  description: "YOU SHALL NOT PASS"
resume-upgrade:
  description: “The wise speak only of what they know.”
"""

GANDALF_DEPS = {
    "gandalf_the_white": {
        "dependencies": {"gandalf_the_grey": ">5"},
        "name": "gandalf",
        "upgrade_supported": ">1.2",
        "version": "7",
    },
}


class GandalfModel(BaseModel):
    gandalf_the_white: DependencyModel


class GandalfUpgrade(DataUpgrade):
    def pre_upgrade_check(self):
        pass

    def log_rollback_instructions(self):
        pass

    def _on_upgrade_granted(self, _):
        pass


class GandalfCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)


@pytest.fixture
def harness():
    harness = Harness(GandalfCharm, meta=GANDALF_METADATA, actions=GANDALF_ACTIONS)
    harness.begin()
    return harness


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("^1.2.3", "1.2.2", False),
        ("^1.2.3", "1.2.3", True),
        ("^1.2.3", "1.2.4", True),
        ("^1.2.3", "1.3.4", True),
        ("^1.2.3", "2.2.3", False),
        ("^1.2.3", "2.0.0", False),
        ("^1.2", "0.9.2", False),
        ("^1.2", "1.0.0", False),
        ("^1.2", "1.2.0", True),
        ("^1.2", "1.3.0", True),
        ("^1.2", "1.3", True),
        ("^1.2", "2", False),
        ("^1.2", "2.2", False),
        ("^1.2", "2.2.0", False),
        ("^1", "0.9.2", False),
        ("^1", "1.0.0", True),
        ("^1", "1.2.0", True),
        ("^1", "1.3.0", True),
        ("^1", "1.3", True),
        ("^1", "2", False),
        ("^1", "2.2", False),
        ("^1", "2.2.0", False),
        ("^0.2.3", "0.2.2", False),
        ("^0.2.3", "0.2.3", True),
        ("^0.2.3", "0.3.0", False),
        ("^0.2.3", "1.2.0", False),
        ("^0.0.3", "0.0.2", False),
        ("^0.0.3", "0.0.4", False),
        ("^0.0.3", "0.1.4", False),
        ("^0.0.3", "1.1.4", False),
        ("^0.0", "0", True),
        ("^0.0", "0.0.1", True),
        ("^0.0", "0.1", False),
        ("^0.0", "1.0", False),
        ("^0", "0", True),
        ("^0", "0.0", True),
        ("^0", "0.0.0", True),
        ("^0", "0.1.0", True),
        ("^0", "0.1.6", True),
        ("^0", "0.1", True),
        ("^0", "1.0", False),
        ("^0", "0.9.9", True),
    ],
)
def test_verify_caret_requirements(requirement, version, output):
    assert verify_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("~1.2.3", "1.2.2", False),
        ("~1.2.3", "1.3.2", False),
        ("~1.2.3", "1.3.5", False),
        ("~1.2.3", "1.2.5", True),
        ("~1.2.3", "1.2", False),
        ("~1.2", "1.2", True),
        ("~1.2", "1.6", False),
        ("~1.2", "1.2.4", True),
        ("~1.2", "1.1", False),
        ("~1.2", "1.0.5", False),
        ("~0.2", "0.2", True),
        ("~0.2", "0.2.3", True),
        ("~0.2", "0.3", False),
        ("~1", "0.3", False),
        ("~1", "1.3", True),
        ("~1", "0.0.9", False),
        ("~1", "0.9.9", False),
        ("~1", "1.9.9", True),
        ("~1", "1.7", True),
        ("~1", "1", True),
        ("~0", "1", False),
        ("~0", "0.1", True),
        ("~0", "0.5.9", True),
    ],
)
def test_verify_tilde_requirements(requirement, version, output):
    assert verify_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("*", "1.5.6", True),
        ("*", "0.0.1", True),
        ("*", "0.2.0", True),
        ("*", "1.0.0", True),
        ("1.*", "1.0.0", True),
        ("1.*", "2.0.0", False),
        ("1.*", "0.6.2", False),
        ("1.2.*", "0.6.2", False),
        ("1.2.*", "1.6.2", False),
        ("1.2.*", "1.2.2", True),
        ("1.2.*", "1.2.0", True),
        ("1.2.*", "1.1.6", False),
        ("1.2.*", "1.1.0", False),
        ("0.2.*", "1.1.0", False),
        ("0.2.*", "0.1.0", False),
        ("0.2.*", "0.2.9", True),
        ("0.2.*", "0.6.0", False),
    ],
)
def test_verify_wildcard_requirements(requirement, version, output):
    assert verify_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("0.1", "0.1", True),
        ("0.1", "0.2", False),
        (">1", "1.8", True),
        (">1", "8.8.0", True),
        (">0", "8.8.0", True),
        (">0", "0.0", False),
        (">0", "0.0.0", False),
        (">0", "0.0.1", True),
        (">1.0", "1.0.0", False),
        (">1.0", "1.0", False),
        (">1.0", "1.5.6", True),
        (">1.0", "2.0", True),
        (">1.0", "0.0.4", False),
        (">1.6", "1.3", False),
        (">1.6", "1.3.8", False),
        (">1.6", "1.35.8", True),
        (">1.6.3", "1.7.8", True),
        (">1.22.3", "1.7.8", False),
        (">0.22.3", "1.7.8", True),
        (">=1.0", "1.0.0", True),
        (">=1.0", "1.0", True),
        (">=0.2", "0.2", True),
        (">=0.2.7", "0.2.7", True),
        (">=1.0", "1.5.6", True),
        (">=1", "1", True),
        (">=1", "1.0", True),
        (">=1", "1.0.0", True),
        (">=1", "1.0.6", True),
        (">=1", "0.0", False),
        (">=1", "0.0.1", False),
        (">=1.0", "2.0", True),
        (">=1.0", "0.0.4", False),
        (">=1.6", "1.3", False),
        (">=1.6", "1.3.8", False),
        (">=1.6", "1.35.8", True),
        (">=1.6.3", "1.7.8", True),
        (">=1.22.3", "1.7.8", False),
        (">=0.22.3", "1.7.8", True),
    ],
)
def test_verify_inequality_requirements(requirement, version, output):
    assert verify_requirements(version=version, requirement=requirement) == output


def test_dependency_model_raises_for_incompatible_version():
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": ">5"},
            "name": "gandalf",
            "upgrade_supported": ">5",
            "version": "4",
        },
    }

    with pytest.raises(ValidationError):
        GandalfModel(**deps)


@pytest.mark.parametrize("value", ["saruman", ""])
def test_dependency_model_raises_for_bad_dependency(value):
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": value},
            "name": "gandalf",
            "upgrade_supported": ">6",
            "version": "7",
        },
    }

    with pytest.raises(ValidationError):
        GandalfModel(**deps)


@pytest.mark.parametrize("value", ["balrog", ""])
def test_dependency_model_raises_for_bad_nested_dependency(value):
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": "~1.0", "bilbo": value},
            "name": "gandalf",
            "upgrade_supported": ">6",
            "version": "7",
        },
    }

    with pytest.raises(ValidationError):
        GandalfModel(**deps)


@pytest.mark.parametrize("value", ["saruman", "1.3", ""])
def test_dependency_model_raises_for_bad_upgrade_supported(value):
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": ">5"},
            "name": "gandalf",
            "upgrade_supported": value,
            "version": "7",
        },
    }

    with pytest.raises(ValidationError):
        GandalfModel(**deps)


def test_dependency_model_succeeds():
    GandalfModel(**GANDALF_DEPS)


def test_dependency_model_succeeds_nested():
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": "~1.0", "bilbo": "^1.2.5"},
            "name": "gandalf",
            "upgrade_supported": ">1.2",
            "version": "7",
        },
    }

    GandalfModel(**deps)


@pytest.mark.parametrize(
    "min_state",
    [
        ("failed"),
        ("idle"),
        ("ready"),
        ("upgrading"),
        ("completed"),
    ],
)
def test_cluster_state(harness, min_state):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)

    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")

    with harness.hooks_disabled():
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/0", {"state": min_state}
        )
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "completed"}
        )

    assert harness.charm.upgrade.cluster_state == min_state


@pytest.mark.parametrize(
    "state",
    [
        ("failed"),
        ("idle"),
        ("ready"),
        ("upgrading"),
        ("completed"),
    ],
)
def test_idle(harness, state):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)

    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")

    with harness.hooks_disabled():
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/0", {"state": state}
        )
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "idle"}
        )

    assert harness.charm.upgrade.idle == (state == "idle")


def test_data_upgrade_raises_on_init(harness):
    # nothing implemented
    class GandalfUpgrade(DataUpgrade):
        pass

    with pytest.raises(TypeError):
        GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)

    # missing pre-upgrade-check
    class GandalfUpgrade(DataUpgrade):
        def log_rollback_instructions(self):
            pass

        def _on_upgrade_granted(self, _):
            pass

    with pytest.raises(TypeError):
        GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)

    # missing missing log-rollback-instructions
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def _on_upgrade_granted(self, _):
            pass

    with pytest.raises(TypeError):
        GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)


def test_on_upgrade_granted_raises_not_implemented_vm(harness, mocker):
    # missing on-upgrade-granted
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)
    with pytest.raises(NotImplementedError):
        mock_event = mocker.MagicMock()
        gandalf._on_upgrade_granted(mock_event)


def test_on_upgrade_granted_succeeds_k8s(harness, mocker):
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel, substrate="k8s")
    mock_event = mocker.MagicMock()
    gandalf._on_upgrade_granted(mock_event)


def test_data_upgrade_succeeds(harness):
    GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)


def test_build_upgrade_stack_raises_not_implemented_vm(harness):
    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)
    with pytest.raises(NotImplementedError):
        gandalf.build_upgrade_stack()


def test_build_upgrade_stack_succeeds_k8s(harness):
    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel, substrate="k8s")
    gandalf.build_upgrade_stack()


def test_set_rolling_update_partition_succeeds_vm(harness):
    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)
    gandalf._set_rolling_update_partition(0)


def test_set_rolling_update_partition_raises_not_implemented_k8s(harness):
    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel, substrate="k8s")
    with pytest.raises(NotImplementedError):
        gandalf._set_rolling_update_partition(0)


def test_set_unit_failed_resets_stack(harness, mocker):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = [0]
    harness.set_leader(True)
    log_spy = mocker.spy(GandalfUpgrade, "log_rollback_instructions")

    assert harness.charm.upgrade._upgrade_stack

    harness.charm.upgrade.set_unit_failed()

    assert not harness.charm.upgrade._upgrade_stack

    assert isinstance(harness.charm.unit.status, BlockedStatus)
    assert log_spy.call_count == 1


@pytest.mark.parametrize("substrate,upgrade_finished_call_count", [("vm", 0), ("k8s", 1)])
def test_set_unit_completed_resets_stack(harness, mocker, substrate, upgrade_finished_call_count):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate=substrate
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = [0]
    harness.set_leader(True)

    assert harness.charm.upgrade._upgrade_stack

    upgrade_finished_spy = mocker.spy(harness.charm.upgrade, "_on_upgrade_finished")

    harness.charm.upgrade.set_unit_completed()

    assert not harness.charm.upgrade._upgrade_stack

    assert upgrade_finished_spy.call_count == upgrade_finished_call_count


def test_upgrade_created_sets_idle_and_deps(harness):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.set_leader(True)

    # relation-created
    harness.add_relation("upgrade", "gandalf")

    assert harness.charm.upgrade.peer_relation
    assert harness.charm.upgrade.peer_relation.data[harness.charm.unit].get("state") == "idle"
    assert (
        json.loads(
            harness.charm.upgrade.peer_relation.data[harness.charm.app].get("dependencies", "")
        )
        == GANDALF_DEPS
    )


def test_pre_upgrade_check_action_fails_non_leader(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_pre_upgrade_check_action(mock_event)

    mock_event.fail.assert_called_once()


def test_pre_upgrade_check_action_fails_already_upgrading(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "ready"})

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_pre_upgrade_check_action(mock_event)

    mock_event.fail.assert_called_once()


def test_pre_upgrade_check_action_runs_pre_upgrade_checks(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)

    mocker.patch.object(harness.charm.upgrade, "pre_upgrade_check")
    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_pre_upgrade_check_action(mock_event)

    harness.charm.upgrade.pre_upgrade_check.assert_called_once()


def test_pre_upgrade_check_action_builds_upgrade_stack_vm(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(charm=harness.charm, dependency_model=gandalf_model)
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)

    mocker.patch.object(harness.charm.upgrade, "build_upgrade_stack", return_value=[1, 2, 3])
    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_pre_upgrade_check_action(mock_event)

    harness.charm.upgrade.build_upgrade_stack.assert_called_once()

    relation_stack = harness.charm.upgrade.peer_relation.data[harness.charm.app].get(
        "upgrade-stack", ""
    )

    assert relation_stack
    assert json.loads(relation_stack) == harness.charm.upgrade.upgrade_stack
    assert json.loads(relation_stack) == [1, 2, 3]


def test_pre_upgrade_check_action_builds_upgrade_stack_k8s(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)

    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "idle"}
    )

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_pre_upgrade_check_action(mock_event)

    relation_stack = harness.charm.upgrade.peer_relation.data[harness.charm.app].get(
        "upgrade-stack", ""
    )

    assert relation_stack
    assert json.loads(relation_stack) == harness.charm.upgrade.upgrade_stack
    assert json.loads(relation_stack) == [0, 1]


def test_pre_upgrade_check_recovers_stack(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    mocker.patch.object(GandalfUpgrade, "_repair_upgrade_stack")

    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "failed"})
    harness.set_leader(True)

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_pre_upgrade_check_action(mock_event)

    GandalfUpgrade._repair_upgrade_stack.assert_called_once()
    assert isinstance(harness.charm.unit.status, BlockedStatus)
    assert harness.charm.upgrade.state == "recovery"


def test_resume_upgrade_action_fails_non_leader(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_resume_upgrade_action(mock_event)

    mock_event.fail.assert_called_once()


def test_resume_upgrade_action_fails_without_upgrade_stack(harness, mocker):
    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    harness.set_leader(True)

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_resume_upgrade_action(mock_event)

    mock_event.fail.assert_called_once()


@pytest.mark.parametrize(
    "upgrade_stack, has_k8s_error, has_succeeded",
    [([0], False, False), ([0, 1, 2], False, False), ([0, 1], False, True), ([0, 1], True, False)],
)
def test_resume_upgrade_action_succeeds_only_when_ran_at_the_right_moment(
    harness, mocker, upgrade_stack, has_k8s_error, has_succeeded
):
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

        def _set_rolling_update_partition(self, partition: int):
            if has_k8s_error:
                raise KubernetesClientError("fake message", "fake cause")

    gandalf_model = GandalfModel(**GANDALF_DEPS)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=gandalf_model, substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    for number in range(1, 3):
        harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, f"gandalf/{number}")

    harness.set_leader(True)

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id,
        "gandalf",
        {"upgrade-stack": json.dumps(upgrade_stack)},
    )

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_resume_upgrade_action(mock_event)

    assert mock_event.fail.call_count == (0 if has_succeeded else 1)
    assert mock_event.set_results.call_count == (1 if has_succeeded else 0)


def test_upgrade_supported_check_fails(harness):
    bad_deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": ">5"},
            "name": "gandalf",
            "upgrade_supported": "~0.2",
            "version": "0.2.1",
        },
    }
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )

    harness.add_relation("upgrade", "gandalf")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf", {"dependencies": json.dumps(bad_deps)}
    )

    with pytest.raises(VersionError):
        harness.charm.upgrade._upgrade_supported_check()


def test_upgrade_supported_check_succeeds(harness):
    good_deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": ">5"},
            "name": "gandalf",
            "upgrade_supported": ">0.2",
            "version": "1.3",
        },
    }
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )

    harness.add_relation("upgrade", "gandalf")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf", {"dependencies": json.dumps(good_deps)}
    )

    harness.charm.upgrade._upgrade_supported_check()


def test_upgrade_charm_runs_checks_on_leader(harness, mocker):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "idle"})
    harness.charm.upgrade.upgrade_stack = [0]

    mocker.patch.object(harness.charm.upgrade, "_upgrade_supported_check")
    harness.charm.on.upgrade_charm.emit()

    harness.charm.upgrade._upgrade_supported_check.assert_called_once()


@pytest.mark.parametrize(
    "substrate,leader,call_count,state",
    [
        ("vm", True, 1, "ready"),
        ("vm", False, 0, "ready"),
        ("k8s", True, 0, "upgrading"),
        ("k8s", False, 0, "upgrading"),
    ],
)
def test_upgrade_charm_runs_upgrade_changed_on_leader_first_to_rollback(
    harness, mocker, substrate, leader, call_count, state
):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate=substrate
    )
    harness.add_relation("upgrade", "gandalf")
    harness.set_leader(leader)
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "recovery"})
    harness.charm.upgrade.upgrade_stack = [2, 1, 0]

    mocker.patch.object(harness.charm.upgrade, "_upgrade_supported_check")
    mocker.patch.object(harness.charm.upgrade, "on_upgrade_changed")
    harness.charm.on.upgrade_charm.emit()

    assert harness.charm.upgrade.on_upgrade_changed.call_count == call_count
    assert harness.charm.upgrade.state == state


@pytest.mark.parametrize(
    "substrate,initial_state,upgrade_stack,final_state",
    [
        ("vm", "idle", [2, 1, 0], "ready"),
        ("vm", "recovery", [2, 0, 1], "ready"),
        ("vm", "recovery", [0, 2, 1], "ready"),
        ("k8s", "recovery", [2, 1, 0], "upgrading"),
    ],
)
def test_upgrade_charm_doesnt_run_upgrade_changed_on_leader_not_first_to_rollback(
    harness, mocker, substrate, initial_state, upgrade_stack, final_state
):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate=substrate
    )
    harness.add_relation("upgrade", "gandalf")
    harness.set_leader(True)
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": initial_state})
    harness.charm.upgrade.upgrade_stack = upgrade_stack

    mocker.patch.object(harness.charm.upgrade, "_upgrade_supported_check")
    mocker.patch.object(harness.charm.upgrade, "on_upgrade_changed")
    harness.charm.on.upgrade_charm.emit()

    harness.charm.upgrade.on_upgrade_changed.assert_not_called()
    assert harness.charm.upgrade.state == final_state


@pytest.mark.parametrize("substrate,state", [("vm", "ready"), ("k8s", "upgrading")])
def test_upgrade_charm_sets_right_state(harness, mocker, substrate, state):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate=substrate
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "idle"})
    harness.charm.upgrade.upgrade_stack = [0]

    mocker.patch.object(harness.charm.upgrade, "_upgrade_supported_check")
    harness.charm.on.upgrade_charm.emit()

    assert harness.charm.upgrade.state == state


def test_upgrade_changed_return_if_recovery(harness, caplog):
    caplog.set_level(logging.DEBUG)
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="vm"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "recovery"})

    with harness.hooks_disabled():
        harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "failed"}
    )

    assert len(caplog.records) == 1
    assert caplog.records[-1].message == "Cluster in recovery, skip..."


def test_upgrade_changed_sets_idle_and_deps_if_all_completed(harness):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    assert not harness.charm.upgrade.stored_dependencies

    harness.charm.upgrade.upgrade_stack = []
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "completed"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.set_leader()
    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "completed"}
    )

    assert harness.charm.upgrade.state == "idle"
    assert harness.charm.upgrade.stored_dependencies == GandalfModel(**GANDALF_DEPS)


def test_upgrade_changed_sets_idle_and_deps_if_some_completed_idle(harness):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")

    assert not harness.charm.upgrade.stored_dependencies == GandalfModel(**GANDALF_DEPS)

    harness.charm.upgrade.upgrade_stack = []
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "completed"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.set_leader()
    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "idle"}
    )

    assert harness.charm.upgrade.state == "idle"
    assert harness.charm.upgrade.stored_dependencies == GandalfModel(**GANDALF_DEPS)


def test_upgrade_changed_does_not_recurse_if_called_all_idle(harness, mocker):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = []
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "idle"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.set_leader(True)

    mocker.patch.object(harness.charm.upgrade, "on_upgrade_changed")
    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "idle"}
    )

    harness.charm.upgrade.on_upgrade_changed.assert_called_once()


@pytest.mark.parametrize(
    "pre_state,stack,call_count,post_state",
    [
        ("idle", [0, 1], 0, "idle"),
        ("idle", [1, 0], 0, "idle"),
        ("ready", [0, 1], 0, "ready"),
        ("ready", [1, 0], 1, "upgrading"),
    ],
)
def test_upgrade_changed_emits_upgrade_granted_only_if_top_of_stack(
    harness, mocker, pre_state, stack, call_count, post_state
):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )

    with harness.hooks_disabled():
        harness.add_relation("upgrade", "gandalf")
        harness.charm.upgrade.upgrade_stack = stack
        harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
        harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": pre_state})

    upgrade_granted_spy = mocker.spy(harness.charm.upgrade, "_on_upgrade_granted")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "ready"}
    )

    assert upgrade_granted_spy.call_count == call_count
    assert harness.charm.upgrade.state == post_state


def test_upgrade_changed_emits_upgrade_granted_only_if_all_ready(harness, mocker):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )

    with harness.hooks_disabled():
        harness.add_relation("upgrade", "gandalf")
        harness.charm.upgrade.upgrade_stack = [1, 0]
        harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
        harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "ready"})

    upgrade_granted_spy = mocker.spy(harness.charm.upgrade, "_on_upgrade_granted")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "idle"}
    )

    assert upgrade_granted_spy.call_count == 0
    assert harness.charm.upgrade.state == "ready"


@pytest.mark.parametrize(
    "substrate,is_leader,unit_number,call_count,has_k8s_error",
    [
        ("vm", False, 1, 0, False),
        ("k8s", True, 3, 0, False),
        ("k8s", True, 0, 0, False),
        ("k8s", True, 1, 1, False),
        ("k8s", True, 2, 1, False),
        ("k8s", False, 1, 1, False),
        ("k8s", False, 1, 1, True),
    ],
)
def test_upgrade_finished_calls_set_rolling_update_partition_only_for_right_units_on_k8s(
    harness, mocker, substrate, is_leader, unit_number, call_count, has_k8s_error
):
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

        def _set_rolling_update_partition(self, partition: int):
            if has_k8s_error:
                raise KubernetesClientError("fake message", "fake cause")

        def set_unit_failed(self):
            pass

    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate=substrate
    )

    with harness.hooks_disabled():
        harness.set_leader(is_leader)
        harness.add_relation("upgrade", "gandalf")
        harness.charm.unit.name = f"gandalf/{unit_number}"
        for number in range(4):
            if number != unit_number:
                harness.add_relation_unit(
                    harness.charm.upgrade.peer_relation.id, f"gandalf/{number}"
                )

    set_partition_spy = mocker.spy(harness.charm.upgrade, "_set_rolling_update_partition")
    set_unit_failed_spy = mocker.spy(harness.charm.upgrade, "set_unit_failed")
    log_rollback_instructions_spy = mocker.spy(harness.charm.upgrade, "log_rollback_instructions")

    mock_event = mocker.MagicMock()
    harness.charm.upgrade._on_upgrade_finished(mock_event)

    assert set_partition_spy.call_count == call_count
    assert set_unit_failed_spy.call_count == (1 if has_k8s_error else 0)
    assert log_rollback_instructions_spy.call_count == (1 if has_k8s_error else 0)


def test_upgrade_changed_recurses_on_leader_and_clears_stack(harness, mocker):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = [0, 1]
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "ready"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.set_leader(True)

    upgrade_changed_spy = mocker.spy(harness.charm.upgrade, "on_upgrade_changed")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "completed"}
    )

    # once for top of stack 1, once for leader 0
    assert upgrade_changed_spy.call_count == 2
    assert harness.charm.upgrade.upgrade_stack == []
    assert json.loads(
        harness.charm.upgrade.peer_relation.data[harness.charm.app].get("upgrade-stack", "")
    ) == [0]


def test_upgrade_changed_does_not_recurse_or_change_stack_non_leader(harness, mocker):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = [0, 1]
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "ready"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")

    upgrade_changed_spy = mocker.spy(harness.charm.upgrade, "on_upgrade_changed")

    harness.update_relation_data(
        harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "completed"}
    )

    # once for top of stack 1
    assert upgrade_changed_spy.call_count == 1
    assert json.loads(
        harness.charm.upgrade.peer_relation.data[harness.charm.app].get("upgrade-stack", "")
    ) == [0, 1]


def test_repair_upgrade_stack_puts_failed_unit_first_in_stack(harness):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = [0, 2]
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "ready"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/2")
    harness.set_leader(True)

    with harness.hooks_disabled():
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "completed"}
        )
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/2", {"state": "failed"}
        )

    harness.charm.upgrade._repair_upgrade_stack()

    assert harness.charm.upgrade.upgrade_stack == [0, 1, 2]


def test_repair_upgrade_stack_does_not_modify_existing_stack(harness):
    harness.charm.upgrade = GandalfUpgrade(
        charm=harness.charm, dependency_model=GandalfModel(**GANDALF_DEPS), substrate="k8s"
    )
    harness.add_relation("upgrade", "gandalf")
    harness.charm.upgrade.upgrade_stack = [0, 2, 1]
    harness.charm.upgrade.peer_relation.data[harness.charm.unit].update({"state": "ready"})
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/1")
    harness.add_relation_unit(harness.charm.upgrade.peer_relation.id, "gandalf/2")
    harness.set_leader(True)

    with harness.hooks_disabled():
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/1", {"state": "failed"}
        )
        harness.update_relation_data(
            harness.charm.upgrade.peer_relation.id, "gandalf/2", {"state": "ready"}
        )

    harness.charm.upgrade._repair_upgrade_stack()

    assert harness.charm.upgrade.upgrade_stack == [0, 2, 1]
