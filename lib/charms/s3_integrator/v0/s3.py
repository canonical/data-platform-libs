# Copyright 2022 Canonical Ltd.
# SPDX-License-Identifier: Apache-2.0
"""A library for communicating with the S3 integrator providers and consumers.

This library provides the relevant interface code implementing the communication
specification for fetching, retrieving, triggering, and responding to events related to
the S3 integrator charm and its consumers.

### Provider charm

The provider is implemented in the `s3-integrator` charm which is meant to be deployed
alongside one or more consumer charms. The provider charm is serving the s3 credential and
metadata needed to communicate and work with an S3 compatible backend.

Example:
```python

from charms.s3_integrator.v0.s3 import CredentialRequestedEvent, S3Provider


class ExampleProviderCharm(CharmBase):
    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.s3_provider = S3Provider(self, "s3-credentials")

        self.framework.observe(self.s3_provider.on.credentials_requested,
            self._on_credential_requested)

    def _on_credential_requested(self, event: CredentialRequestedEvent):
        if not self.unit.is_leader():
            return

        # get relation id
        relation_id = event.relation.id

        # S3 configuration parameters
        desired_configuration = {"access-key": "your-access-key", "secret-key":
            "your-secret-key", "bucket": "your-bucket"}

        # update the configuration
        self.s3_provider.update_credential_data(relation_id, desired_configuration)

        # or it is possible to set each field independently

        self.s3_provider.set_secret_key(relation_id, "your-secret-key")


if __name__ == "__main__":
    main(ExampleProviderCharm)


### Requirer charm

The requirer charm is the charm requiring the the S3 credentials.
An example of requirer charm is the following:

Example:
```python

from charms.s3_integrator.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Consumer
)

class ExampleRequirerCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)
        self.s3_client = S3Consumer(self, "s3-credentials")

        self.framework.observe(self.s3_client.on.credentials_changed, self._on_credential_changed)
        self.framework.observe(self.s3_client.on.credentials_gone, self._on_credential_gone)

    def _on_credential_changed(self, event: CredentialsChangedEvent):

        # access single parameter credential
        secret_key = event.secret_key
        access_key = event.access_key

        # or as alternative all credentials can be collected as a dictionary
        credentials = self.s3_client.get_s3_credentials()

    def _on_credential_gone(self, event: CredentialsGoneEvent):
        # credentials are removed
        pass

 if __name__ == "__main__":
    main(ExampleRequirerCharm)
```

"""
import logging
from typing import Dict, List, Optional

import ops.charm
import ops.framework
import ops.model
from ops.charm import (
    EventSource,
    Object,
    ObjectEvents,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationEvent,
)
from ops.model import Relation

LIBID = "55c4cfd4a8ac45b1b62f2826272a1cf8"
LIBAPI = 0
LIBPATCH = 1

logger = logging.getLogger(__name__)


class CredentialRequestedEvent(RelationEvent):
    """Event emitted when a set of credential is requested for use on this relation."""


class S3CredentialEvents(ObjectEvents):
    """Event descriptor for events raised by S3Provider."""

    credentials_requested = EventSource(CredentialRequestedEvent)


class S3Provider(Object):
    """A provider handler for communicating S3 credentials to consumers."""

    on = S3CredentialEvents()

    def __init__(
        self,
        charm: ops.charm.CharmBase,
        relation_name: str,
    ):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.local_app = self.charm.model.app
        self.local_unit = self.charm.unit
        self.relation_name = relation_name

        # monitor relation changed event for changes in the credentials
        self.framework.observe(charm.on[relation_name].relation_changed, self._on_relation_changed)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """React to the relation joined event by consuming data."""
        if not self.charm.unit.is_leader():
            return
        # emit on credential request event
        self.on.credentials_requested.emit(event.relation, app=event.app, unit=event.unit)

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
                key: value for key, value in relation.data[self.charm.app].items()
            }
        return data

    def update_credential_data(self, relation_id: int, data: dict) -> None:
        """Updates the credential data as set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            data: dict containing the key-value pairs
                that should be updated.
        """
        # check and write changes only if you are the leader
        if self.local_unit.is_leader():
            # get relation
            relation = self.charm.model.get_relation(self.relation_name, relation_id)

            if not relation:
                return

            need_credential_data = False
            # iterate over the provided data
            for k, v in data.items():
                if k in relation.data[self.local_app]:
                    # check if value of a speficied key has been modified
                    if v == relation.data[self.local_app][k]:
                        continue
                need_credential_data = True

            # update the databag only if some of the fields changed
            if need_credential_data:
                relation.data[self.local_app].update(data)
                logger.debug(f"Updated S3 credentials: {data}")

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])

    def set_bucket(self, relation_id: int, bucket: str) -> None:
        """Set bucket name.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            bucket: the bucket name.
        """
        self.update_credential_data(relation_id, {"bucket": bucket})

    def set_access_key(self, relation_id: int, access_key: str) -> None:
        """Set access-key value.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            access_key: the access-key value.
        """
        self.update_credential_data(relation_id, {"access-key": access_key})

    def set_secret_key(self, relation_id: int, secret_key: str) -> None:
        """Set the secret key value.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            secret_key: the value of the secret key.
        """
        self.update_credential_data(relation_id, {"secret-key": secret_key})

    def set_path(self, relation_id: int, path: str) -> None:
        """Set the path.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            path: the path value.
        """
        self.update_credential_data(relation_id, {"path": path})

    def set_endpoint(self, relation_id: int, endpoint: str) -> None:
        """Set the endpoint address.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            endpoint: the endpoint address.
        """
        self.update_credential_data(relation_id, {"endpoint": endpoint})

    def set_region(self, relation_id: int, region: str) -> None:
        """Set the region location.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            region: the region address.
        """
        self.update_credential_data(relation_id, {"region": region})

    def set_s3_uri_style(self, relation_id: int, s3_uri_style: str) -> None:
        """Set the S3 URI style.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            s3_uri_style: the s3 URI style.
        """
        self.update_credential_data(relation_id, {"s3-uri-style": s3_uri_style})

    def set_storage_class(self, relation_id: int, storage_class: str) -> None:
        """Set the storage class.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            storage_class: the storage class.
        """
        self.update_credential_data(relation_id, {"storage-class": storage_class})

    def set_tls_ca_chain(self, relation_id: int, tls_ca_chain: str) -> None:
        """Set the tls_ca_chain value.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            tls_ca_chain: the TLS Chain value.
        """
        self.update_credential_data(relation_id, {"tls-ca-chain": tls_ca_chain})

    def set_s3_api_version(self, relation_id: int, s3_api_version: str) -> None:
        """Set the S3 API verion.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            s3_api_version: the S3 version value.
        """
        self.update_credential_data(relation_id, {"s3-api-version": s3_api_version})

    def set_attributes(self, relation_id: int, attributes: str) -> None:
        """Set the connection attributes.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            attributes: the attributes value.
        """
        self.update_credential_data(relation_id, {"attributes": attributes})


class S3Event(RelationEvent):
    """Base class for S3 storage events."""

    @property
    def bucket(self) -> Optional[str]:
        """Returns the bucket name."""
        return self.relation.data[self.relation.app].get("bucket", default=None)

    @property
    def access_key(self) -> Optional[str]:
        """Returns the access key."""
        return self.relation.data[self.relation.app].get("access-key", default=None)

    @property
    def secret_key(self) -> Optional[str]:
        """Returns the secret key."""
        return self.relation.data[self.relation.app].get("secret-key", default=None)

    @property
    def path(self) -> Optional[str]:
        """Returns the path where data can be stored."""
        return self.relation.data[self.relation.app].get("path", default=None)

    @property
    def endpoint(self) -> Optional[str]:
        """Returns the enpoint address."""
        return self.relation.data[self.relation.app].get("endpoint", default=None)

    @property
    def region(self) -> Optional[str]:
        """Returns the region."""
        return self.relation.data[self.relation.app].get("region", default=None)

    @property
    def s3_uri_style(self) -> Optional[str]:
        """Returns the s3 uri style."""
        return self.relation.data[self.relation.app].get("s3-uri-style", default=None)

    @property
    def storage_class(self) -> Optional[str]:
        """Returns the storage class name."""
        return self.relation.data[self.relation.app].get("storage-class", default=None)

    @property
    def tls_ca_chain(self) -> Optional[str]:
        """Returns the TLS CA chain."""
        return self.relation.data[self.relation.app].get("tls-ca-chain", default=None)

    @property
    def s3_api_version(self) -> Optional[str]:
        """Returns the S3 API version."""
        return self.relation.data[self.relation.app].get("s3-api-version", default=None)

    @property
    def attributes(self) -> Optional[str]:
        """Returns the attributes."""
        return self.relation.data[self.relation.app].get("attributes", default=None)


class CredentialsChangedEvent(S3Event):
    """Event emitted when S3 credential are used on this relation."""


class CredentialsGoneEvent(RelationEvent):
    """Event emitted when S3 credential are removed from this relation."""


class S3CredentialRequiresEvents(ObjectEvents):
    """Event descriptor for events raised by the S3Provider."""

    credentials_changed = EventSource(CredentialsChangedEvent)
    credentials_gone = EventSource(CredentialsGoneEvent)


S3_REQUIRED_OPTIONS = ["access-key", "secret-key", "bucket"]


class S3Consumer(Object):
    """Requires-side of the s3 relation."""

    on = S3CredentialRequiresEvents()

    def __init__(
        self,
        charm: ops.charm.CharmBase,
        relation_name: str,
    ):
        """Manager of the s3 client relations."""
        super().__init__(charm, relation_name)

        self.relation_name = relation_name
        self.charm = charm
        self.local_app = charm.model.app
        self.local_unit = self.charm.unit
        self.framework.observe(
            self.charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )

        self.framework.observe(
            self.charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Notify the charm about the presence of S3 credentials."""
        # check if the mandatory options are in the relation data
        contains_required_options = True
        # get current credentials data
        credentials = self.get_s3_credentials()
        # records missing options
        missing_options = []
        for configuration_option in S3_REQUIRED_OPTIONS:
            if configuration_option not in credentials:
                contains_required_options = False
                missing_options.append(configuration_option)
        # emit credential change event only if all mandatory fields are present
        if contains_required_options:
            self.on.credentials_changed.emit(event.relation, app=event.app, unit=event.unit)
        else:
            logger.info(
                f"Some mandatory fields: {missing_options} are not present, do not emit credential change event!"
            )

    def get_s3_credentials(self) -> Dict:
        """Return the s3 credentials as a dictionary."""
        relation = self.charm.model.get_relation(self.relation_name)
        if not relation:
            return {}
        return relation.data[relation.app]

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Notify the charm about a broken S3 credential store relation."""
        self.on.credentials_gone.emit(event.relation, app=event.app, unit=event.unit)

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])
