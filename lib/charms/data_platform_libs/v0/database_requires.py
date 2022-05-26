# Copyright 2022 Canonical Ltd.
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

"""Relation requirer side abstraction for database relation.

This library is mostly an uniform interface to a selection of common databases
metadata, with added custom events that add convenience to database management,
and methods to consume the application related data.

Following an example of using the DatabaseCreatedEvent, in the context of the
application charm code:

```python

from charms.data_platform_libs.v0.database_requires import DatabaseRequires

class ApplicationCharm(CharmBase):
    # Application charm that connects to database charms.

    def __init__(self, *args):
        super().__init__(*args)

        # Charm events defined in the database requires charm library.
        self.database = DatabaseRequires(self, "database")
        self.framework.observe(self.database.on.database_created, self._on_database_created)

    def _on_database_created(self, _) -> None:
        # Handle the created database

        # Create configuration file for app
        config_file = self._render_app_config_file(
            self.database.username,
            self.database.password,
            self.database.endpoints,
        )

        # Start application with rendered configuration
        self._start_application(config_file)

        # Set active status
        self.unit.status = ActiveStatus("received database credentials")
```
"""

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

        # TODO: evaluate the possibility of losing the diff if some error
        # happens in the charm before the diff is completely checked (DPE-412).
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
            a string value stored in the relation data bag for
                the specified key.
        """
        return self.relation.data[self.relation.app].get(key, None)

    @property
    def endpoints(self) -> str:
        """Returns a comma separated list of read/write endpoints."""
        return self._get_relation_data("endpoints")

    @property
    def password(self) -> str:
        """Returns the password for the created user."""
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
        """Returns the replicaset name.

        MongoDB only.
        """
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
        """Returns the connection URIs.

        MongoDB, Redis, OpenSearch and Kafka only.
        """
        return self._get_relation_data("uris")

    @property
    def username(self) -> str:
        """Returns the created username."""
        credentials = self._get_relation_data("credentials")
        if credentials is None:
            return None
        return json.loads(credentials)["username"]

    @property
    def version(self) -> str:
        """Returns the version of the database.

        Version as informed by the database daemon.
        """
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
        """Set value for key in the application relation databag.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            key: key to write the date in the relation.
            value: value of the data to write in the relation.
        """
        self.relation.data[self.charm.model.app][key] = value
