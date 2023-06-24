# Copyright 2023 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Handler for `upgrade` relation events for in-place upgrades on VMs."""

import json
import logging
from abc import abstractmethod
from typing import Callable, Generator

from ops.charm import (
    ActionEvent,
    CharmBase,
    RelationChangedEvent,
    RelationCreatedEvent,
    UpgradeCharmEvent,
)
from ops.framework import EventBase, Object
from ops.model import Relation
from pydantic import BaseModel, root_validator

# The unique Charmhub library identifier, never change it
LIBID = "156258aefb79435a93d933409a8c8684"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)

# --- DEPENDENCY RESOLUTION FUNCTIONS ---


def build_complete_sem_ver(version: str) -> list[int]:
    """Builds complete major.minor.patch version from version string.

    Returns:
        List of major.minor.patch version integers
    """
    versions = [int(ver) if ver != "*" else 0 for ver in str(version).split(".")]

    # padding with 0s until complete major.minor.patch
    return (versions + 3 * [0])[:3]


def verify_caret_requirements(version: str, requirement: str) -> bool:
    """Verifies version requirements using carats.

    Args:
        version: the version currently in use
        requirement: the requirement version

    Returns:
        True if `version` meets defined `requirement`. Otherwise False
    """
    if not requirement.startswith("^"):
        return True
    else:
        requirement = requirement[1:]

    sem_version = build_complete_sem_ver(version)
    sem_requirement = build_complete_sem_ver(requirement)

    # caret uses first non-zero character, not enough to just count '.
    max_version_index = requirement.count(".")
    for i, semver in enumerate(sem_requirement):
        if semver != 0:
            max_version_index = i
            break

    for i in range(3):
        # version higher than first non-zero
        if (i < max_version_index) and (sem_version[i] > sem_requirement[i]):
            return False

        # version either higher or lower than first non-zero
        if (i == max_version_index) and (sem_version[i] != sem_requirement[i]):
            return False

        # valid
        if (i > max_version_index) and (sem_version[i] > sem_requirement[i]):
            return True

    return False


def verify_tilde_requirements(version: str, requirement: str) -> bool:
    """Verifies version requirements using tildes.

    Args:
        version: the version currently in use
        requirement: the requirement version

    Returns:
        True if `version` meets defined `requirement`. Otherwise False
    """
    if not requirement.startswith("~"):
        return True
    else:
        requirement = requirement[1:]

    sem_version = build_complete_sem_ver(version)
    sem_requirement = build_complete_sem_ver(requirement)

    max_version_index = min(1, requirement.count("."))

    for i in range(3):
        # version higher before requirement level
        if (i < max_version_index) and (sem_version[i] > sem_requirement[i]):
            return False

        # version either higher or lower at requirement level
        if (i == max_version_index) and (sem_version[i] != sem_requirement[i]):
            return False

        # version lower after requirement level
        if (i > max_version_index) and (sem_version[i] < sem_requirement[i]):
            return False

    # must be valid
    return True


def verify_wildcard_requirements(version: str, requirement: str) -> bool:
    """Verifies version requirements using wildcards.

    Args:
        version: the version currently in use
        requirement: the requirement version

    Returns:
        True if `version` meets defined `requirement`. Otherwise False
    """
    if "*" not in requirement:
        return True

    sem_version = build_complete_sem_ver(version)
    sem_requirement = build_complete_sem_ver(requirement)

    max_version_index = requirement.count(".")

    for i in range(3):
        # version not the same before wildcard
        if (i < max_version_index) and (sem_version[i] != sem_requirement[i]):
            return False

        # version not higher after wildcard
        if (i == max_version_index) and (sem_version[i] < sem_requirement[i]):
            return False

    # must be valid
    return True


def verify_inequality_requirements(version: str, requirement: str) -> bool:
    """Verifies version requirements using inequalities.

    Args:
        version: the version currently in use
        requirement: the requirement version

    Returns:
        True if `version` meets defined `requirement`. Otherwise False
    """
    if not any([char for char in [">", ">="] if requirement.startswith(char)]):
        return True
    else:
        raw_requirement = requirement.replace(">", "").replace("=", "")

    sem_version = build_complete_sem_ver(version)
    sem_requirement = build_complete_sem_ver(raw_requirement)

    max_version_index = raw_requirement.count(".") or 0

    for i in range(3):
        # valid at same requirement level
        if (
            (i == max_version_index)
            and ("=" in requirement)
            and (sem_version[i] == sem_requirement[i])
        ):
            return True

        # version not increased at any point
        if sem_version[i] < sem_requirement[i]:
            return False

        # valid
        if sem_version[i] > sem_requirement[i]:
            return True

    # must not be valid
    return False


def verify_requirements(version: str, requirement: str) -> bool:
    """Verifies a specified version against defined requirements.

    Supports caret (^), tilde (~), wildcard (*) and greater-than inequalities (>, >=)

    Args:
        version: the version currently in use
        requirement: the requirement version

    Returns:
        True if `version` meets defined `requirement`. Otherwise False
    """
    if not all(
        [
            verify_inequality_requirements(version=version, requirement=requirement),
            verify_caret_requirements(version=version, requirement=requirement),
            verify_tilde_requirements(version=version, requirement=requirement),
            verify_wildcard_requirements(version=version, requirement=requirement),
        ]
    ):
        return False

    return True


# --- DEPENDENCY MODEL TYPES ---


class Dependency(str):
    """Type for a single dependency.

    Examples values:
        - >0.0.3
        - >=1.5
        - ~3.6
        - ^6.8.0
        - 4.*
    """

    chars = ["~", "^", ">", "*"]

    @classmethod
    def __get_validators__(cls) -> Generator[Callable, None, None]:
        """Get validators to parse and validate the input data."""
        yield cls.validate

    @classmethod
    def validate(cls, value: str):
        """Validates input requirement uses only one supported special character."""
        if (count := sum([value.count(char) for char in cls.chars])) > 1:
            raise ValueError(
                f"Value uses greater than 1 special character (^ ~ > *). Found {count}."
            )

        return value


class DependencyModel(BaseModel):
    """Manager for a single dependency.

    To be used as part of another model representing a collection of arbitrary dependencies.

    Example Usage:
        ```python
        class KafkaDependenciesModel(BaseModel):
            kafka_charm: DependencyModel
            kafka_service: DependencyModel

        deps = {
            "kafka_charm": {
                "dependencies": {"zookeeper": ">5"},
                "name": "kafka",
                "upgrade_supported": ">5",
                "version": "10",
            },
            "kafka_service": {
                "dependencies": {"zookeeper": "^3.6"},
                "name": "kafka",
                "upgrade_supported": "~3.3",
                "version": "3.3.2",
            },
        }

        # loading dict in to model
        model = KafkaDependenciesModel(**deps)

        # exporting back validated deps
        print(model.dict())
        ```
    """

    dependencies: dict[str, Dependency]
    name: str
    upgrade_supported = Dependency
    version: str

    @root_validator(pre=True)
    @classmethod
    def upgrade_supported_validator(
        cls, values
    ) -> dict[str, dict[str, Dependency] | str | Dependency]:
        """Validates specified `version` meets `upgrade_supported` requirement."""
        if not verify_requirements(
            version=values.get("version"), requirement=values.get("upgrade_supported")
        ):
            raise ValueError(
                f"upgrade_supported value {values.get('upgrade_supported')} greater than version value {values.get('version')} for {values.get('name')}."
            )

        return values

    # TODO: implement when comparing two dependency models for upgradability
    def __rshift__(self):
        """Used for comparing two instances of `DependencyModel` for upgradability.

        Example Usage:
            ```python
            old_deps: DependencyModel = foo  # from persisted relation data
            new_deps: DependencyModel = bar  # from charm

            if not old_deps >> new_deps:
                raise
            ```
        """
        raise NotImplementedError


# --- CUSTOM EXCEPTIONS ---


class UpgradeError(Exception):
    """Base class for upgrade related exceptions in the module."""

    def __init__(self, message: str, cause: str | None, resolution: str | None):
        super().__init__(message)
        self.message = message
        self.cause = cause or ""
        self.resolution = resolution or ""

    def __repr__(self):
        """Representation of the UpgradeError class."""
        return f"{type(self).__module__}.{type(self).__name__} - {str(vars(self))}"

    def __str__(self):
        """String representation of the UpgradeError class."""
        return repr(self)


class ClusterNotReadyError(UpgradeError):
    """Exception flagging that the cluster is not ready to start upgrading.

    Args:
        `message`: string message to be logged out
        `cause`: short human-readable description of the cause of the error
        `resolution`: short human-readable instructions for manual error resolution (optional)
    """

    def __init__(self, message: str, cause: str, resolution: str | None = None):
        super().__init__(message, cause=cause, resolution=resolution)


class UpgradeFailedError(UpgradeError):
    """Exception flagging that something in the upgrade process failed, and should be aborted.

    Args:
        `message`: string message to be logged out
        `cause`: short human-readable description of the cause of the error
        `resolution`: short human-readable instructions for manual solutions to the error
    """

    def __init__(self, message: str, cause: str, resolution: str):
        super().__init__(message, cause=cause, resolution=resolution)


# --- CUSTOM EVENTS ---


class UpgradeGrantedEvent(EventBase):
    """Used to tell units that they can process an upgrade."""


# --- EVENT HANDLER ---


class DataUpgrade(Object):
    """Manages `upgrade` relation operators for in-place upgrades."""

    def __init__(
        self,
        charm: CharmBase,
        dependency_model: BaseModel,
        relation_name: str = "upgrade",
    ):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.dependency_model = dependency_model
        self.relation_name = relation_name

        # events
        self.framework.observe(
            self.charm.on[relation_name].relation_created, self._on_upgrade_created
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_changed, self._on_upgrade_changed
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_changed, self._on_upgrade_changed
        )

        # actions
        self.framework.observe(
            getattr(self.on, "pre_upgrade_check_action"), self._on_pre_upgrade_check_action
        )

    @property
    def peer_relation(self) -> Relation | None:
        """The upgrade peer relation."""
        return self.charm.model.get_relation(self.relation_name)

    @property
    def state(self) -> str | None:
        """The unit state from the upgrade peer relation."""
        if not self.peer_relation:
            return None

        return self.peer_relation.data[self.charm.unit].get("state", None)

    @property
    def stored_dependencies(self) -> BaseModel | None:
        """The application dependencies from the upgrade peer relation."""
        if not self.peer_relation:
            return None

        if not (deps := self.peer_relation.data[self.charm.app].get("dependencies", "")):
            return None

        return type(self.dependency_model)(**json.loads(deps))

    @property
    def upgrade_stack(self) -> list[int] | None:
        """The upgrade stack from the upgrade peer relation."""
        if not self.peer_relation:
            return None

        return (
            json.loads(self.peer_relation.data[self.charm.app].get("upgrade-stack", "[]")) or None
        )

    @abstractmethod
    def pre_upgrade_check(self) -> None:
        """Runs necessary checks validating the cluster is in a healthy state to upgrade.

        Raises:
            ClusterNotReadyError: if cluster is not ready to upgrade
        """
        pass

    @abstractmethod
    def build_upgrade_stack(self) -> list[int]:
        """Builds ordered list of all application unit.ids to upgrade in.

        Returns:
            List of integeter unit.ids, ordered by upgrade order
                e.g [5, 2, 4, 1, 3]
        """
        pass

    def _on_upgrade_created(self, _: RelationCreatedEvent) -> None:
        """Handler for `upgrade-relation-created` events."""
        if not self.charm.unit.is_leader() or not self.peer_relation:
            return

        # persisting dependencies to the relation data
        self.peer_relation.data[self.charm.app].update(
            {"dependencies": json.dumps(self.dependency_model.dict())}
        )

    def _on_pre_upgrade_check_action(self, event: ActionEvent):
        """Handler for `pre-upgrade-check-action` events."""
        if not self.peer_relation:
            event.fail(message="Could not find upgrade relation.")
            return

        if not self.charm.unit.is_leader():
            event.fail(message="Action must be ran on the Juju leader.")
            return

        # checking if upgrade in progress
        for unit in set([self.charm.unit] + list(self.peer_relation.units)):
            if (current_state := self.peer_relation.data[unit].get("state")) in {
                "ready",
                "upgrading",
                "completed",
                "failed",
            }:
                event.fail(f"{unit} is in {current_state} state and is currently upgrading.")
                return

        try:
            self.pre_upgrade_check()
            upgrade_stack = self.build_upgrade_stack()
        except ClusterNotReadyError as e:
            logger.error(e)
            event.fail(message=e.message)
            return
        except Exception as e:
            logger.error(e)
            event.fail(message="Unknown error found.")
            return

        # setting upgrade_stack to peer relation data
        self.peer_relation.data[self.charm.app].update(
            {"upgrade-stack": json.dumps(upgrade_stack)}
        )

        # flagging units as healthy and waiting for incoming upgrade-charm event
        for unit in set([self.charm.unit] + list(self.peer_relation.units)):
            self.peer_relation.data[unit].update({"state": "waiting"})

    # TODO - implement
    def _on_upgrade_changed(self, event: RelationChangedEvent):
        """Handler for `upgrade-relation-changed` events."""
        raise NotImplementedError

    # TODO - implement
    def _on_upgrade_charm(self, event: UpgradeCharmEvent):
        """Handler for `upgrade-charm` events."""
        raise NotImplementedError
