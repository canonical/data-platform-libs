from abc import abstractmethod
from ops.charm import (
    ActionEvent,
    CharmBase,
    RelationChangedEvent,
    UpgradeCharmEvent,
)
from ops.framework import EventBase, Object
from overrides import override
import logging


# The unique Charmhub library identifier, never change it
LIBID = "156258aefb79435a93d933409a8c8684"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


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
        `resolution`: short human-readable instructions for manual solutions to the error (optional)
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


class UpgradeGrantedEvent(EventBase):
    """Used to tell units that they can process an upgrade."""


class DataUpgrade(Object):
    """Manages `upgrade` relations."""

    def __init__(self, charm: CharmBase, relation_name: str = "upgrade"):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

        self.framework.observe(
            self.charm.on[relation_name].relation_changed, self._on_upgrade_changed
        )

    def _on_upgrade_changed(self, event: RelationChangedEvent):
        raise NotImplementedError

    def _on_pre_upgrade_check_action(self, event: ActionEvent):
        if not self.charm.unit.is_leader():
            event.fail(message="Action must be ran on the Juju leader for the application.")
            return

        try:
            self.pre_upgrade_check()
        except ClusterNotReadyError as e:
            logger.error(e)
            event.fail(message="Action must be ran on the Juju leader for the application.")

    def _on_upgrade_charm(self, event: UpgradeCharmEvent):
        raise NotImplementedError

    @abstractmethod
    def pre_upgrade_check(self) -> None:
        """Runs necessary checks validating the cluster is in a healthy state to upgrade.

        Raises:
            ClusterNotReadyError: if cluster is not ready to upgrade
        """
        pass

    @abstractmethod
    def build_upgrade_stack(self) -> list[str]:
        """Builds ordered list of all application unit.ids to upgrade in.

        Returns:
            List of integeter unit.ids, ordered by upgrade order
                e.g [5, 2, 4, 1, 3]
        """
        pass
