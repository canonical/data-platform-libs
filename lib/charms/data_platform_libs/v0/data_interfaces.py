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

"""TODO: Add a proper docstring here.

This is a placeholder docstring for this charm library. Docstrings are
presented on Charmhub and updated whenever you push a new version of the
library.

Complete documentation about creating and documenting libraries can be found
in the SDK docs at https://juju.is/docs/sdk/libraries.

See `charmcraft publish-lib` and `charmcraft fetch-lib` for details of how to
share and consume charm libraries. They serve to enhance collaboration
between charmers. Use a charmer's libraries for classes that handle
integration with their charm.

Bear in mind that new revisions of the different major API versions (v0, v1,
v2 etc) are maintained independently.  You can continue to update v0 and v1
after you have pushed v3.

Markdown is supported, following the CommonMark specification.
"""

import json
import logging
from abc import ABC, ABCMeta, abstractmethod
from collections import namedtuple
from datetime import datetime
from typing import List, Optional

from ops.charm import (
    CharmBase,
    CharmEvents,
    RelationChangedEvent,
    RelationEvent,
    RelationJoinedEvent,
)
from ops.framework import EventSource, Object, _Metaclass
from ops.model import Relation

# The unique Charmhub library identifier, never change it
LIBID = "6c3e6b6680d64e9c89e611d1a15f65be"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class ExtraRoleEvent(RelationEvent):
    """Base class for data events."""

    @property
    def extra_user_roles(self) -> Optional[str]:
        """Returns the extra user roles that were requested."""
        return self.relation.data[self.relation.app].get("extra-user-roles")


# Database related events and fields


class DatabaseProvidesEvent(RelationEvent):
    """Base class for database events."""

    @property
    def database(self) -> Optional[str]:
        """Returns the database that was requested."""
        return self.relation.data[self.relation.app].get("database")


class DatabaseRequestedEvent(DatabaseProvidesEvent, ExtraRoleEvent):
    """Event emitted when a new database is requested for use on this relation."""


class DatabaseProvidesEvents(CharmEvents):
    """Database events.

    This class defines the events that the database can emit.
    """

    database_requested = EventSource(DatabaseRequestedEvent)


# Kafka related events


class KafkaProvidesEvent(RelationEvent):
    """Base class for Kafka events."""

    @property
    def topic(self) -> Optional[str]:
        """Returns the topic that was requested."""
        return self.relation.data[self.relation.app].get("topic")


class TopicRequestedEvent(KafkaProvidesEvent, ExtraRoleEvent):
    """Event emitted when a new topic is requested for use on this relation."""


class KafkaProvidesEvents(CharmEvents):
    """Kafka events.

    This class defines the events that the Kafka can emit.
    """

    topic_requested = EventSource(TopicRequestedEvent)


# Zookeeper related events


class ZookeeperProvidesEvent(RelationEvent):
    """Base class for Zookeeper events."""

    @property
    def chroot(self) -> Optional[str]:
        """Returns the chroot that was requested."""
        return self.relation.data[self.relation.app].get("chroot")


class ChrootRequestedEvent(ZookeeperProvidesEvent, ExtraRoleEvent):
    """Event emitted when a new chtoot is requested for use on this relation."""


class ZookeeperProvidesEvents(CharmEvents):
    """Zookeeper events.

    This class defines the events that the Zookeeper can emit.
    """

    chroot_requested = EventSource(ChrootRequestedEvent)


Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.

added - keys that were added
changed - keys that still exist but have new values
deleted - key that were deleted"""


def diff(event: RelationChangedEvent, bucket: str) -> Diff:
    """Retrieves the diff of the data in the relation changed databag.

    Args:
        event: relation changed event.
        bucket: bucket of the databag (app or unit)

    Returns:
        a Diff instance containing the added, deleted and changed
            keys from the event relation databag.
    """
    # Retrieve the old data from the data key in the application relation databag.
    old_data = json.loads(event.relation.data[bucket].get("data", "{}"))
    # Retrieve the new data from the event relation databag.
    new_data = {
        key: value for key, value in event.relation.data[event.app].items() if key != "data"
    }

    # These are the keys that were added to the databag and triggered this event.
    added = new_data.keys() - old_data.keys()
    # These are the keys that were removed from the databag and triggered this event.
    deleted = old_data.keys() - new_data.keys()
    # These are the keys that already existed in the databag,
    # but had their values changed.
    changed = {key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]}
    # Convert the new_data to a serializable format and save it for a next diff check.
    event.relation.data[bucket].update({"data": json.dumps(new_data)})

    # Return the diff with all possible changes.
    return Diff(added, changed, deleted)


class _AbstractMetaclass(ABCMeta, _Metaclass):
    """Meta class."""

    pass


class DataProvides(Object, ABC, metaclass=_AbstractMetaclass):
    """Base provides-side of the data products relation."""

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        super().__init__(charm, relation_name)
        self.charm = charm
        self.local_app = self.charm.model.app
        self.local_unit = self.charm.unit
        self.relation_name = relation_name
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
        return diff(event, self.local_app)

    @abstractmethod
    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the database relation has changed."""
        raise NotImplementedError

    def fetch_relation_data(self) -> dict:
        """Retrieves data from relation.

        This function can be used to retrieve data from a relation
        in the charm code when outside an event callback.

        Returns:
            a dict of the values stored in the relation data bag
                for all relation instances (indexed by the relation id).
        """
        data = {}
        for relation in self.relations:
            data[relation.id] = {
                key: value for key, value in relation.data[relation.app].items() if key != "data"
            }
        return data

    def _update_relation_data(self, relation_id: int, data: dict) -> None:
        """Updates a set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            data: dict containing the key-value pairs
                that should be updated in the relation.
        """
        if self.local_unit.is_leader():
            relation = self.charm.model.get_relation(self.relation_name, relation_id)
            relation.data[self.local_app].update(data)

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])

    def set_credentials(self, relation_id: int, username: str, password: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            username: user that was created.
            password: password of the created user.
        """
        self._update_relation_data(
            relation_id,
            {
                "username": username,
                "password": password,
            },
        )

    def set_tls(self, relation_id: int, tls: str) -> None:
        """Set whether TLS is enabled.

        Args:
            relation_id: the identifier for a particular relation.
            tls: whether tls is enabled (True or False).
        """
        self._update_relation_data(relation_id, {"tls": tls})

    def set_tls_ca(self, relation_id: int, tls_ca: str) -> None:
        """Set the TLS CA in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            tls_ca: TLS certification authority.
        """
        self._update_relation_data(relation_id, {"tls_ca": tls_ca})


class DatabaseProvides(DataProvides):
    """Provider-side of the database relations."""

    on = DatabaseProvidesEvents()

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        super().__init__(charm, relation_name)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        # Only the leader should handle this event.
        if not self.local_unit.is_leader():
            return

        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Emit a database requested event if the setup key (database name and optional
        # extra user roles) was added to the relation databag by the application.
        if "database" in diff.added:
            self.on.database_requested.emit(event.relation, app=event.app, unit=event.unit)

    def set_endpoints(self, relation_id: int, connection_strings: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data(relation_id, {"endpoints": connection_strings})

    def set_read_only_endpoints(self, relation_id: int, connection_strings: str) -> None:
        """Set database replicas connection strings.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data(relation_id, {"read-only-endpoints": connection_strings})

    def set_replset(self, relation_id: int, replset: str) -> None:
        """Set replica set name in the application relation databag.

        MongoDB only.

        Args:
            relation_id: the identifier for a particular relation.
            replset: replica set name.
        """
        self._update_relation_data(relation_id, {"replset": replset})

    def set_uris(self, relation_id: int, uris: str) -> None:
        """Set the database connection URIs in the application relation databag.

        MongoDB, Redis, OpenSearch and Kafka only.

        Args:
            relation_id: the identifier for a particular relation.
            uris: connection URIs.
        """
        self._update_relation_data(relation_id, {"uris": uris})

    def set_version(self, relation_id: int, version: str) -> None:
        """Set the database version in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            version: database version.
        """
        self._update_relation_data(relation_id, {"version": version})


class KafkaProvides(DataProvides):
    """Provider-side of the Kafka relation."""

    on = KafkaProvidesEvents()

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        super().__init__(charm, relation_name)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        # Only the leader should handle this event.
        if not self.local_unit.is_leader():
            return

        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Emit a database requested event if the setup key (database name and optional
        # extra user roles) was added to the relation databag by the application.
        if "topic" in diff.added:
            self.on.topic_requested.emit(event.relation, app=event.app, unit=event.unit)

    def set_bootstrap_server(self, relation_id: int, bootstrap_server: str) -> None:
        """Set the bootstrap server in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            bootstrap_server: the bootstrap server address.
        """
        self._update_relation_data(relation_id, {"endpoints": bootstrap_server})

    def set_consumer_group_prefix(self, relation_id: int, consumer_group_prefix: str) -> None:
        """Set the bootstrap server in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            consumer_group_prefix: the consumer group prefix string.
        """
        self._update_relation_data(relation_id, {"consumer-group-prefix": consumer_group_prefix})

    def set_zookeeper_uris(self, relation_id: int, zookeeper_uris: str) -> None:
        """Set the bootstrap server in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            zookeeper_uris: comma-seperated list of ZooKeeper server uris.
        """
        self._update_relation_data(relation_id, {"zookeeper-uris": zookeeper_uris})


class ZookeeperProvides(DataProvides):
    """Provider-side of the Zookeeper relation."""

    on = ZookeeperProvidesEvents()

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        super().__init__(charm, relation_name)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        # Only the leader should handle this event.
        if not self.local_unit.is_leader():
            return

        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Emit a database requested event if the setup key (database name and optional
        # extra user roles) was added to the relation databag by the application.
        if "chroot" in diff.added:
            self.on.chroot_requested.emit(event.relation, app=event.app, unit=event.unit)

    def set_endpoints(self, relation_id: int, connection_strings: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data(relation_id, {"endpoints": connection_strings})


class BaseRequiresEvent(RelationEvent):
    """Base class requires for events."""

    @property
    def username(self) -> Optional[str]:
        """Returns the created username."""
        return self.relation.data[self.relation.app].get("username")

    @property
    def password(self) -> Optional[str]:
        """Returns the password for the created user."""
        return self.relation.data[self.relation.app].get("password")

    @property
    def tls(self) -> Optional[str]:
        """Returns whether TLS is configured."""
        return self.relation.data[self.relation.app].get("tls")

    @property
    def tls_ca(self) -> Optional[str]:
        """Returns TLS CA."""
        return self.relation.data[self.relation.app].get("tls-ca")


# Database events


class DatabaseRequiresEvent(RelationEvent):
    """Base class for database events."""

    @property
    def endpoints(self) -> Optional[str]:
        """Returns a comma separated list of read/write endpoints."""
        return self.relation.data[self.relation.app].get("endpoints")

    @property
    def read_only_endpoints(self) -> Optional[str]:
        """Returns a comma separated list of read only endpoints."""
        return self.relation.data[self.relation.app].get("read-only-endpoints")

    @property
    def replset(self) -> Optional[str]:
        """Returns the replicaset name.

        MongoDB only.
        """
        return self.relation.data[self.relation.app].get("replset")

    @property
    def uris(self) -> Optional[str]:
        """Returns the connection URIs.

        MongoDB, Redis, OpenSearch.
        """
        return self.relation.data[self.relation.app].get("uris")

    @property
    def version(self) -> Optional[str]:
        """Returns the version of the database.

        Version as informed by the database daemon.
        """
        return self.relation.data[self.relation.app].get("version")


class DatabaseCreatedEvent(BaseRequiresEvent, DatabaseRequiresEvent):
    """Event emitted when a new database is created for use on this relation."""


class DatabaseEndpointsChangedEvent(BaseRequiresEvent, DatabaseRequiresEvent):
    """Event emitted when the read/write endpoints are changed."""


class DatabaseReadOnlyEndpointsChangedEvent(BaseRequiresEvent, DatabaseRequiresEvent):
    """Event emitted when the read only endpoints are changed."""


class DatabaseRequiresEvents(CharmEvents):
    """Database events.

    This class defines the events that the database can emit.
    """

    database_created = EventSource(DatabaseCreatedEvent)
    endpoints_changed = EventSource(DatabaseEndpointsChangedEvent)
    read_only_endpoints_changed = EventSource(DatabaseReadOnlyEndpointsChangedEvent)


# Kafka events


class KafkaRequiresEvent(RelationEvent):
    """Base class for Kafka events."""

    @property
    def bootstrap_server(self) -> Optional[str]:
        """Returns a a comma-seperated list of broker uris."""
        return self.relation.data[self.relation.app].get("endpoints")

    @property
    def consumer_group_prefix(self) -> Optional[str]:
        """Returns the consumer-group-prefix."""
        return self.relation.data[self.relation.app].get("consumer-group-prefix")

    @property
    def zookeeper_uris(self) -> Optional[str]:
        """Returns a comma separated list of Zookeeper uris."""
        return self.relation.data[self.relation.app].get("zookeeper-uris")


class TopicCreatedEvent(BaseRequiresEvent, KafkaRequiresEvent):
    """Event emitted when a new topic is created for use on this relation."""


class BootstrapServerChangedEvent(BaseRequiresEvent, KafkaRequiresEvent):
    """Event emitted when the bootstrap server is changed."""


class KakfaCredentialsChangedEvent(BaseRequiresEvent, KafkaRequiresEvent):
    """Event emitted when the Kafka credentials(username or password) are changed."""


class KafkaRequiresEvents(CharmEvents):
    """Kafka events.

    This class defines the events that the Kafka can emit.
    """

    topic_created = EventSource(TopicCreatedEvent)
    bootstrap_server_changed = EventSource(BootstrapServerChangedEvent)
    credentials_changed = EventSource(KakfaCredentialsChangedEvent)


# Zookeeper events


class ZookeeperRequiresEvent(RelationEvent):
    """Base class for Zookeeper events."""

    @property
    def endpoints(self) -> Optional[str]:
        """Returns a comma separated list of read/write endpoints."""
        return self.relation.data[self.relation.app].get("endpoints")


class ChrootCreatedEvent(BaseRequiresEvent, ZookeeperRequiresEvent):
    """Event emitted when a new chroot is created for use on this relation."""


class ZookeeperEndpointsChangedEvent(BaseRequiresEvent, ZookeeperRequiresEvent):
    """Event emitted when the endpoints are changed."""


class ZookeeperCredentialsChangedEvent(BaseRequiresEvent, ZookeeperRequiresEvent):
    """Event emitted when the Kafka credentials(username or password) are changed."""


class ZookeeperRequiresEvents(CharmEvents):
    """Zookeeper events.

    This class defines the events that the Zookeeper can emit.
    """

    chroot_created = EventSource(ChrootCreatedEvent)
    endpoints_changed = EventSource(ZookeeperEndpointsChangedEvent)
    credentials_changed = EventSource(ZookeeperCredentialsChangedEvent)


Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.

— added — keys that were added.
— changed — keys that still exist but have new values.
— deleted — keys that were deleted.
"""


class _AbstractMetaclass(ABCMeta, _Metaclass):
    pass


class BaseRequires(Object, ABC, metaclass=_AbstractMetaclass):
    """Requires-side of the database relation."""

    def __init__(
        self,
        charm,
        relation_name: str,
        extra_user_roles: str = None,
    ):
        """Manager of base client relations."""
        super().__init__(charm, relation_name)
        self.charm = charm
        self.extra_user_roles = extra_user_roles
        self.local_app = self.charm.model.app
        self.local_unit = self.charm.unit
        self.relation_name = relation_name
        self.framework.observe(
            self.charm.on[relation_name].relation_joined, self._on_relation_joined_event
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_changed, self._on_relation_changed_event
        )

    @abstractmethod
    def _on_relation_joined_event(self, event: RelationJoinedEvent) -> None:
        """Event emitted when the application joins the database relation."""
        raise NotImplementedError

    @abstractmethod
    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        raise NotImplementedError

    def fetch_relation_data(self) -> dict:
        """Retrieves data from relation.

        This function can be used to retrieve data from a relation
        in the charm code when outside an event callback.

        Returns:
            a dict of the values stored in the relation data bag
                for all relation instances (indexed by the relation ID).
        """
        data = {}
        for relation in self.relations:
            data[relation.id] = {
                key: value for key, value in relation.data[relation.app].items() if key != "data"
            }
        return data

    def _update_relation_data(self, relation_id: int, data: dict) -> None:
        """Updates a set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            data: dict containing the key-value pairs
                that should be updated in the relation.
        """
        if self.local_unit.is_leader():
            relation = self.charm.model.get_relation(self.relation_name, relation_id)
            relation.data[self.local_app].update(data)

    def _diff(self, event: RelationChangedEvent) -> Diff:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a Diff instance containing the added, deleted and changed
                keys from the event relation databag.
        """
        return diff(event, self.local_unit)

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])


# Database Requires


class DatabaseRequires(BaseRequires):
    """Requires-side of the database relation."""

    on = DatabaseRequiresEvents()

    def __init__(
        self,
        charm,
        relation_name: str,
        database_name: str,
        extra_user_roles: str = None,
        relations_aliases: List[str] = None,
    ):
        """Manager of database client relations."""
        super().__init__(charm, relation_name, extra_user_roles)
        self.database = database_name
        self.relations_aliases = relations_aliases

        # Define custom event names for each alias.
        if relations_aliases:
            # Ensure the number of aliases does not exceed the maximum
            # of connections allowed in the specific relation.
            relation_connection_limit = self.charm.meta.requires[relation_name].limit
            if len(relations_aliases) != relation_connection_limit:
                raise ValueError(
                    f"The number of aliases must match the maximum number of connections allowed in the relation. "
                    f"Expected {relation_connection_limit}, got {len(relations_aliases)}"
                )

            for relation_alias in relations_aliases:
                self.on.define_event(f"{relation_alias}_database_created", DatabaseCreatedEvent)
                self.on.define_event(
                    f"{relation_alias}_endpoints_changed", DatabaseEndpointsChangedEvent
                )
                self.on.define_event(
                    f"{relation_alias}_read_only_endpoints_changed",
                    DatabaseReadOnlyEndpointsChangedEvent,
                )

    def _assign_relation_alias(self, relation_id: int) -> None:
        """Assigns an alias to a relation.

        This function writes in the unit data bag.

        Args:
            relation_id: the identifier for a particular relation.
        """
        # If no aliases were provided, return immediately.
        if not self.relations_aliases:
            return

        # Return if an alias was already assigned to this relation
        # (like when there are more than one unit joining the relation).
        if (
            self.charm.model.get_relation(self.relation_name, relation_id)
            .data[self.local_unit]
            .get("alias")
        ):
            return

        # Retrieve the available aliases (the ones that weren't assigned to any relation).
        available_aliases = self.relations_aliases[:]
        for relation in self.charm.model.relations[self.relation_name]:
            alias = relation.data[self.local_unit].get("alias")
            if alias:
                logger.debug("Alias %s was already assigned to relation %d", alias, relation.id)
                available_aliases.remove(alias)

        # Set the alias in the unit relation databag of the specific relation.
        relation = self.charm.model.get_relation(self.relation_name, relation_id)
        relation.data[self.local_unit].update({"alias": available_aliases[0]})

    def _emit_aliased_event(self, event: RelationChangedEvent, event_name: str) -> None:
        """Emit an aliased event to a particular relation if it has an alias.

        Args:
            event: the relation changed event that was received.
            event_name: the name of the event to emit.
        """
        alias = self._get_relation_alias(event.relation.id)
        if alias:
            getattr(self.on, f"{alias}_{event_name}").emit(
                event.relation, app=event.app, unit=event.unit
            )

    def _get_relation_alias(self, relation_id: int) -> Optional[str]:
        """Returns the relation alias.

        Args:
            relation_id: the identifier for a particular relation.

        Returns:
            the relation alias or None if the relation was not found.
        """
        for relation in self.charm.model.relations[self.relation_name]:
            if relation.id == relation_id:
                return relation.data[self.local_unit].get("alias")
        return None

    def _on_relation_joined_event(self, event: RelationJoinedEvent) -> None:
        """Event emitted when the application joins the database relation."""
        # If relations aliases were provided, assign one to the relation.
        self._assign_relation_alias(event.relation.id)

        # Sets both database and extra user roles in the relation
        # if the roles are provided. Otherwise, sets only the database.
        if self.extra_user_roles:
            self._update_relation_data(
                event.relation.id,
                {
                    "database": self.database,
                    "extra-user-roles": self.extra_user_roles,
                },
            )
        else:
            self._update_relation_data(event.relation.id, {"database": self.database})

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the database relation has changed."""
        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Check if the database is created
        # (the database charm shared the credentials).
        if "username" in diff.added and "password" in diff.added:
            # Emit the default event (the one without an alias).
            logger.info("database created at %s", datetime.now())
            self.on.database_created.emit(event.relation, app=event.app, unit=event.unit)

            # Emit the aliased event (if any).
            self._emit_aliased_event(event, "database_created")

            # To avoid unnecessary application restarts do not trigger
            # “endpoints_changed“ event if “database_created“ is triggered.
            return

        # Emit an endpoints changed event if the database
        # added or changed this info in the relation databag.
        if "endpoints" in diff.added or "endpoints" in diff.changed:
            # Emit the default event (the one without an alias).
            logger.info("endpoints changed on %s", datetime.now())
            self.on.endpoints_changed.emit(event.relation, app=event.app, unit=event.unit)

            # Emit the aliased event (if any).
            self._emit_aliased_event(event, "endpoints_changed")

            # To avoid unnecessary application restarts do not trigger
            # “read_only_endpoints_changed“ event if “endpoints_changed“ is triggered.
            return

        # Emit a read only endpoints changed event if the database
        # added or changed this info in the relation databag.
        if "read-only-endpoints" in diff.added or "read-only-endpoints" in diff.changed:
            # Emit the default event (the one without an alias).
            logger.info("read-only-endpoints changed on %s", datetime.now())
            self.on.read_only_endpoints_changed.emit(
                event.relation, app=event.app, unit=event.unit
            )

            # Emit the aliased event (if any).
            self._emit_aliased_event(event, "read_only_endpoints_changed")


class KafkaRequires(BaseRequires):
    """Requires-side of the Kafka relation."""

    on = KafkaRequiresEvents()

    def __init__(self, charm, relation_name: str, topic: str, extra_user_roles: str = None):
        """Manager of Kafka client relations."""
        # super().__init__(charm, relation_name)
        super().__init__(charm, relation_name, extra_user_roles)
        self.charm = charm
        self.topic = topic

    def _on_relation_joined_event(self, event: RelationJoinedEvent) -> None:
        """Event emitted when the application joins the Kafka relation."""
        # Sets both topic and extra user roles in the relation
        # if the roles are provided. Otherwise, sets only the topic.
        self._update_relation_data(
            event.relation.id,
            {
                "topic": self.topic,
                "extra-user-roles": self.extra_user_roles,
            }
            if self.extra_user_roles is not None
            else {"topic": self.topic},
        )

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the Kafka relation has changed."""
        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Check if the topic is created
        # (the Kafka charm shared the credentials).
        if "username" in diff.added and "password" in diff.added:
            # Emit the default event (the one without an alias).
            logger.info("topic created at %s", datetime.now())
            self.on.topic_created.emit(event.relation, app=event.app, unit=event.unit)

            # To avoid unnecessary application restarts do not trigger
            # “endpoints_changed“ event if “topic_created“ is triggered.
            return

        # Emit an endpoints (bootstap-server) changed event if the Kakfa endpoints
        # added or changed this info in the relation databag.
        if "endpoints" in diff.added or "endpoints" in diff.changed:
            # Emit the default event (the one without an alias).
            logger.info("endpoints changed on %s", datetime.now())
            self.on.bootstrap_server_changed.emit(
                event.relation, app=event.app, unit=event.unit
            )  # here check if this is the right design
            return

        # Emit a read only credential changed event if the kafka credentials
        # changed this info in the relation databag.
        if "username" in diff.changed or "password" in diff.changed:

            logger.info("credential changed on %s", datetime.now())
            self.on.credentials_changed.emit(event.relation, app=event.app, unit=event.unit)
            return


class ZookeeperRequires(BaseRequires):
    """Requires-side of the Kafka relation."""

    on = ZookeeperRequiresEvents()

    def __init__(self, charm, relation_name: str, chroot: str, extra_user_roles: str = None):
        """Manager of Kafka client relations."""
        # super().__init__(charm, relation_name)
        super().__init__(charm, relation_name, extra_user_roles)
        self.charm = charm
        self.chroot = chroot

    def _on_relation_joined_event(self, event: RelationJoinedEvent) -> None:
        """Event emitted when the application joins the zookeeper relation."""
        # Sets both Zookeeper and extra user roles in the relation
        # if the roles are provided. Otherwise, sets only the chroot.

        self._update_relation_data(
            event.relation.id,
            {
                "chroot": self.chroot,
                "extra-user-roles": self.extra_user_roles,
            }
            if self.extra_user_roles is not None
            else {"topic": self.chroot},
        )

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the Zookeeper relation has changed."""
        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Check if the topic is created
        # (the Zookeeper charm shared the credentials).
        if "username" in diff.added and "password" in diff.added:
            # Emit the default event (the one without an alias).
            logger.info("chroot created at %s", datetime.now())
            self.on.chroot_created.emit(event.relation, app=event.app, unit=event.unit)

            # To avoid unnecessary application restarts do not trigger
            # “endpoints_changed“ event if “chroot_created“ is triggered.
            return

        # Emit an endpoints changed event if the Zookeeper endpoints
        # added or changed this info in the relation databag.
        if "endpoints" in diff.added or "endpoints" in diff.changed:
            # Emit the default event (the one without an alias).
            logger.info("endpoints changed on %s", datetime.now())
            self.on.endpoints_changed.emit(event.relation, app=event.app, unit=event.unit)
            return

        # Emit a read only credential changed event if the Zookeeper credentials
        # changed this info in the relation databag.
        if "username" in diff.changed or "password" in diff.changed:

            logger.info("credential changed on %s", datetime.now())
            self.on.credentials_changed.emit(event.relation, app=event.app, unit=event.unit)
            return
