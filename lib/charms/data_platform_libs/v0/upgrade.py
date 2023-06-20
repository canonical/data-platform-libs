from ops.charm import ActionEvent, CharmBase, RelationChangedEvent, RelationEvent, UpgradeCharmEvent
from ops.framework import EventBase, Object


# The unique Charmhub library identifier, never change it
LIBID = "156258aefb79435a93d933409a8c8684"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

class UpgradeGrantedEvent(EventBase):
    """Used to tell units that they can process an upgrade."""

class DataUpgrade(Object):
    """Manages `upgrade` relations."""

    def __init__(self, charm: CharmBase, relation_name: str = "upgrade"):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

        self.framework.observe(self.charm.on[relation_name].relation_changed, self._on_upgrade_changed)

    def _on_upgrade_changed(self, event: RelationChangedEvent):
        raise NotImplementedError

    def _on_pre_upgrade_check_action(self, event: ActionEvent):
        raise NotImplementedError

    def _on_upgrade_charm(self, event: UpgradeCharmEvent):
        raise NotImplementedError
