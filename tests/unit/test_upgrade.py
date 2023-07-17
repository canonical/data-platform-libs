# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from ops.charm import CharmBase
from ops.testing import Harness
from pydantic import ValidationError

from charms.data_platform_libs.v0.upgrade import (
    BaseModel,
    DataUpgrade,
    DependencyModel,
    build_complete_sem_ver,
    verify_caret_requirements,
    verify_inequality_requirements,
    verify_tilde_requirements,
    verify_wildcard_requirements,
)

GANDALF_METADATA = """
name: gandalf
peers:
  upgrade:
    interface: upgrade
"""

GANDALF_ACTIONS = """
pre-upgrade-check:
  description: "YOU SHALL NOT PASS"
"""


class GandalfModel(BaseModel):
    gandalf_the_white: DependencyModel


class GandalfCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)


@pytest.fixture
def harness():
    harness = Harness(GandalfCharm, meta=GANDALF_METADATA, actions=GANDALF_ACTIONS)
    harness.begin()
    return harness


@pytest.mark.parametrize(
    "version,output",
    [
        ("0.0.24.0.4", [0, 0, 24]),
        ("3.5.3", [3, 5, 3]),
        ("0.3", [0, 3, 0]),
        ("1.2", [1, 2, 0]),
        ("3.5.*", [3, 5, 0]),
        ("0.*", [0, 0, 0]),
        ("1.*", [1, 0, 0]),
        ("1.2.*", [1, 2, 0]),
        ("*", [0, 0, 0]),
        (1, [1, 0, 0]),
    ],
)
def test_build_complete_sem_ver(version, output):
    assert build_complete_sem_ver(version) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("~1.2.3", "1.2.2", True),
        ("1.2.3", "1.2.2", True),
        ("^1.2.3", "1.2.2", False),
        ("^1.2.3", "1.3", True),
        ("^1.2.3", "2.2.5", False),
        ("^1.2", "1.2.2", True),
        ("^1.2.3", "1.2", False),
        ("^1.2.3", "2.2.5", False),
        ("^1", "1.2.2", True),
        ("^1", "1.6", True),
        ("^1", "1.7.9", True),
        ("^1", "0.6", False),
        ("^1", "2", False),
        ("^1", "2.3", False),
        ("^0.2.3", "0.2.2", False),
        ("^0.2.3", "0.2.5", True),
        ("^0.2.3", "1.2.5", False),
        ("^0.2.3", "0.3.6", False),
        ("^0.0.3", "0.0.4", False),
        ("^0.0.3", "0.0.2", False),
        ("^0.0.3", "0.0", False),
        ("^0.0.3", "0.3.6", False),
        ("^0.0", "0.0.3", True),
        ("^0.0", "0.1.0", False),
        ("^0", "0.1.0", True),
        ("^0", "0.3.6", True),
        ("^0", "1.0.0", False),
    ],
)
def test_verify_caret_requirements(requirement, version, output):
    assert verify_caret_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("^1.2.3", "1.2.2", True),
        ("1.2.3", "1.2.2", True),
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
    assert verify_tilde_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("~1", "1", True),
        ("^0", "1", True),
        ("0", "0.1", True),
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
    assert verify_wildcard_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("~1", "1", True),
        ("^0", "1", True),
        ("0", "0.1", True),
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
    assert verify_inequality_requirements(version=version, requirement=requirement) == output


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


@pytest.mark.parametrize("value", ["saruman", "1.3", ""])
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


@pytest.mark.parametrize("value", ["balrog", "1.3", ""])
def test_dependency_model_raises_for_bad_nested_dependency(value):
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": "~1.0", "durin": value},
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
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": ">5"},
            "name": "gandalf",
            "upgrade_supported": ">1.2",
            "version": "7",
        },
    }

    GandalfModel(**deps)


def test_dependency_model_succeeds_nested():
    deps = {
        "gandalf_the_white": {
            "dependencies": {"gandalf_the_grey": "~1.0", "durin": "^1.2.5"},
            "name": "gandalf",
            "upgrade_supported": ">1.2",
            "version": "7",
        },
    }

    GandalfModel(**deps)


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

    # missing on-upgrade-granted
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

    with pytest.raises(TypeError):
        GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)

    # all present
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

        def _on_upgrade_granted(self, _):
            pass

    GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)


def test_build_upgrade_stack_raises_not_implemented_vm(harness):
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

        def _on_upgrade_granted(self, _):
            pass

    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel)

    with pytest.raises(NotImplementedError):
        gandalf.build_upgrade_stack()


def test_build_upgrade_stack_succeeds_k8s(harness):
    class GandalfUpgrade(DataUpgrade):
        def pre_upgrade_check(self):
            pass

        def log_rollback_instructions(self):
            pass

        def _on_upgrade_granted(self, _):
            pass

    gandalf = GandalfUpgrade(charm=harness.charm, dependency_model=GandalfModel, substrate="k8s")

    gandalf.build_upgrade_stack()
