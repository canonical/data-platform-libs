#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""TODO: Add a proper docstring here."""

import json
import logging
from datetime import datetime

from ops.charm import CharmEvents, RelationChangedEvent, RelationEvent
from ops.framework import EventSource, Object

# The unique Charmhub library identifier, never change it
LIBID = "0241e088ffa9440fb4e3126349b2fb62"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class DatabaseCreatedEvent(RelationEvent):
    """Event emitted when a new database is created for use on this relation."""

    pass


class DatabaseEndpointsChangedEvent(RelationEvent):
    """Event emitted when the read/write endpoints are changed."""

    pass


class DatabaseReadOnlyEndpointsChangedEvent(RelationEvent):
    """Event emitted when the read only endpoints are changed."""

    pass


class DatabaseEvents(CharmEvents):
    """Database events.

    This class defines the events that the database can emit.
    """

    database_created = EventSource(DatabaseCreatedEvent)
    endpoints_changed = EventSource(DatabaseEndpointsChangedEvent)
    read_only_endpoints_changed = EventSource(DatabaseReadOnlyEndpointsChangedEvent)


class DatabaseRequires(Object):
    """Requires-side of the database relation."""

    on = DatabaseEvents()

    def __init__(self, charm, relation_name: str = "database"):
        """Manager of database client relations."""
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation = self.charm.model.get_relation(relation_name)
        self.framework.observe(
            self.charm.on[relation_name].relation_changed, self._on_relation_changed_event
        )

    def _diff(self, event: RelationChangedEvent) -> dict:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a dict containing the added, deleted and changed
                keys from the event relation databag.
        """
        # Retrieve the old data from the data key in the application relation databag.
        old_data = json.loads(self.relation.data[self.charm.model.app].get("data", "{}"))
        # Retrieve the new data from the event relation databag.
        new_data = event.relation.data[event.app]

        # These are the keys that were added to the databag and triggered this event.
        added = new_data.keys() - old_data.keys()
        # These are the keys that were removed from the databag and triggered this event.
        deleted = old_data.keys() - new_data.keys()
        # These are the keys that already existed in the databag,
        # but had their values changed.
        changed = {
            key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]
        }

        # Convert the new_data to a serializable format and save it for a next diff check.
        data = {key: value for key, value in new_data.items() if key != "data"}
        self._update_relation_data("data", json.dumps(data))

        # Return the diff with all possible changes.
        return {
            "added": added,
            "changed": changed,
            "deleted": deleted,
        }

    def _get_relation_data(self, key: str) -> str:
        """Retrieves data from relation.

        Args:
            key: key to retrieve the data from the relation.

        Returns:
            value stored in the relation data bag for
                the specified key.
        """
        return self.relation.data[self.relation.app].get(key, None)

    @property
    def endpoints(self) -> str:
        """Returns a comma separated list of read/write endpoints."""
        return self._get_relation_data("endpoints")

    @property
    def password(self) -> str:
        """Returns the password of the created user."""
        credentials = self._get_relation_data("credentials")
        if credentials is None:
            return None
        return json.loads(credentials)["password"]

    @property
    def read_only_endpoints(self) -> str:
        """Returns a comma separated list of read only endpoints."""
        return self._get_relation_data("read-only-endpoints")

    @property
    def replset(self) -> str:
        """Returns the replicaset name (MongoDB only)."""
        return self._get_relation_data("replset")

    @property
    def tls(self) -> str:
        """Returns whether TLS is configured."""
        return self._get_relation_data("tls")

    @property
    def tls_ca(self) -> str:
        """Returns TLS CA."""
        return self._get_relation_data("tls-ca")

    @property
    def uris(self) -> str:
        """Returns the connection URIs (MongoDB, Redis, OpenSearch and Kafka only)."""
        return self._get_relation_data("uris")

    @property
    def username(self) -> str:
        """Returns the username that was created."""
        credentials = self._get_relation_data("credentials")
        if credentials is None:
            return None
        return json.loads(credentials)["username"]

    @property
    def version(self) -> str:
        """Returns the version of the database."""
        return self._get_relation_data("version")

    def _on_relation_changed_event(self, event: RelationChangedEvent):
        diff = self._diff(event)

        # Check if the database is created
        # (the database charm shared the credentials).
        if "credentials" in diff["added"]:
            self.on.database_created.emit(event.relation)

        # Emit an endpoints changed event if the database
        # added or changed this info in the relation databag.
        if "endpoints" in diff["added"] or "endpoints" in diff["changed"]:
            logger.info(f"endpoints changed on {datetime.now()}")
            self.on.endpoints_changed.emit(event.relation)

        # Emit a read only endpoints changed event if the database
        # added or changed this info in the relation databag.
        if "read-only-endpoints" in diff["added"] or "read-only-endpoints" in diff["changed"]:
            logger.info(f"read-only-endpoints changed on {datetime.now()}")
            self.on.read_only_endpoints_changed.emit(event.relation)

    def set_database(self, database: str) -> None:
        """Set database name."""
        self._update_relation_data("database", database)

    def set_extra_user_roles(self, extra_user_roles: str) -> None:
        """Request extra user roles."""
        self._update_relation_data("extra-user-roles", extra_user_roles)

    def _update_relation_data(self, key: str, value: str) -> None:
        """Set PostgreSQL primary connection string.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            key: key to write the date in the relation.
            value: value of the data to write in the relation.
        """
        self.relation.data[self.charm.model.app][key] = value
