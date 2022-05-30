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

"""Relation provider side abstraction for database relation.

This library is a uniform interface to a selection of common database
metadata, with added custom events that add convenience to database management,
and methods to set the application related data.

Following an example of using the DatabaseRequestedEvent, in the context of the
database charm code:

```python
from charms.data_platform_libs.v0.database_provides import DatabaseProvides

class SampleCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        # Charm events defined in the database provides charm library.
        self.provided_database = DatabaseProvides(self, relation_name="database")
        self.framework.observe(self.provided_database.on.database_requested,
            self._on_database_requested)

        # Database generic helper
        self.database = DatabaseHelper()

    def _on_database_requested(self, _) -> None:
        # Handle the event triggered by a new database requested in the relation

        # Retrieve the database name using the charm library.
        db_name = self.provided_database.database

        # generate a new user credential
        username = self.database.generate_user()
        password = self.database.generate_password()

        # set the credentials for the relation
        self.provided_database.set_credentials(username, password)

        # set other variables for the relation self.provided_database.set_tls("False")
```
"""

import json
import logging
from collections import namedtuple
from typing import Optional

from ops.charm import CharmBase, CharmEvents, RelationChangedEvent, RelationEvent
from ops.framework import EventSource, Object

# The unique Charmhub library identifier, never change it
LIBID = "8eea9ca584d84c7bb357f1946b6f34ce"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class DatabaseRequestedEvent(RelationEvent):
    """Event emitted when a new database is requested for use on this relation."""


class DatabaseEvents(CharmEvents):
    """Database events.

    This class defines the events that the database can emit.
    """

    database_requested = EventSource(DatabaseRequestedEvent)


Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.

added - keys that were added
changed - keys that still exist but have new values
deleted - key that were deleted"""


class DatabaseProvides(Object):
    """Provides-side of the database relation."""

    on = DatabaseEvents()

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation = self.charm.model.get_relation(relation_name)
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self._on_relation_changed,
        )

    def _diff(self, event: RelationChangedEvent) -> Diff:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a Diff instance containing the added, deleted and changed
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
        self._update_relation_data({"data": json.dumps(data)})

        # Return the diff with all possible changes.
        return Diff(added, changed, deleted)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the database relation has changed."""
        # Only the leader should handle this event.
        if not self.charm.unit.is_leader():
            return

        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Emit a database requested event if the setup key (database name and optional
        # extra user roles) was added to the relation databag by the application.
        if "database" in diff.added:
            self.on.database_requested.emit(event.relation)

    def _get_relation_data(self, key: str) -> Optional[str]:
        """Retrieves data from relation.

        Args:
            key: key to retrieve the data from the relation.

        Returns:
            value stored in the relation data bag for
                the specified key or None if the key doesn't exist.
        """
        return self.relation.data[self.relation.app].get(key)

    @property
    def database(self) -> Optional[str]:
        """Returns the database that was requested."""
        return self._get_relation_data("database")

    @property
    def extra_user_roles(self) -> Optional[str]:
        """Returns the extra user roles that were requested."""
        return self._get_relation_data("extra-user-roles")

    @property
    def username(self) -> Optional[str]:
        """Returns the username that was created."""
        # Retrieve the credentials key from the provides side databag
        # as it was set on this side.
        return self.relation.data[self.charm.model.app].get("username")

    def set_credentials(self, username: str, password: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            username: user that was created.
            password: password of the created user.
        """
        self._update_relation_data(
            {
                "username": username,
                "password": password,
            }
        )

    def set_endpoints(self, connection_strings: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data({"endpoints": connection_strings})

    def set_read_only_endpoints(self, connection_strings: str) -> None:
        """Set database replicas connection strings.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data({"read-only-endpoints": connection_strings})

    def set_replset(self, replset: str) -> None:
        """Set replica set name in the application relation databag.

        MongoDB only.

        Args:
            replset: replica set name.
        """
        self._update_relation_data({"replset": replset})

    def set_tls(self, tls: str) -> None:
        """Set whether TLS is enabled.

        Args:
            tls: whether tls is enabled (True or False).
        """
        self._update_relation_data({"tls": tls})

    def set_tls_ca(self, tls_ca: str) -> None:
        """Set the TLS CA in the application relation databag.

        Args:
            tls_ca: TLS certification authority.
        """
        self._update_relation_data({"tls_ca": tls_ca})

    def set_uris(self, uris: str) -> None:
        """Set the database connection URIs in the application relation databag.

        MongoDB, Redis, OpenSearch and Kafka only.

        Args:
            uris: connection URIs.
        """
        self._update_relation_data({"uris": uris})

    def set_version(self, version: str) -> None:
        """Set the database version in the application relation databag.

        Args:
            version: database version.
        """
        self._update_relation_data({"version": version})

    def _update_relation_data(self, data: dict) -> None:
        """Updates a set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            data: dict containing the key-value pairs
                that should be updated in the relation.
        """
        if self.charm.unit.is_leader():
            self.relation.data[self.charm.model.app].update(data)
