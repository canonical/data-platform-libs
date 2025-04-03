# Copyright 2025 Canonical Ltd.
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

r"""Library for creating service accounts that are configured to run Spark jobs.

This library contains the SparkServiceAccountProvider and SparkServiceAccountRequirer
classes for handling the relation between charms that require Spark Service Account
to be created in order to function, and charms that create and provide them.

### SparkServiceAccountRequirer

Following is an example of using the SparkServiceAccountRequirer class in the context
of the application charm code:

```python
import json

from charms.data_platform_libs.v0.spark_service_account import (
    SparkServiceAccountRequirer,
    ServiceAccountGrantedEvent,
    ServiceAccountPropertyChangedEvent,
    ServiceAccountGoneEvent
)
from ops.model import ActiveStatus, BlockedStatus


class RequirerCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        namespace, username = "default", "test"
        self.spark_service_account_requirer = SparkServiceAccountRequirer(self, relation_name="service-account", service_account=f"{namespace}:{username}", )
        self.framework.observe(
            self.spark_service_account_requirer.on.account_granted, self._on_account_granted
        )
        self.framework.observe(
            self.spark_service_account_requirer.on.account_gone, self._on_account_gone
        )
        self.framework.observe(
            self.spark_service_account_requirer.on.properties_changed, self._on_spark_properties_changed
        )

    def _on_account_granted(self, event: ServiceAccountGrantedEvent):
        # Handle the account_granted event

        namespace, username = event.service_account.split(":")

        # Create configuration file for app
        config_file = self._render_app_config_file(
            namespace=namespace,
            username=username,
        )

        # Start application with rendered configuration
        self._start_application(config_file)

        # Set appropriate status
        self.unit.status = ActiveStatus("Received Spark service account")

    def _on_spark_properties_changed(self, event: ServiceAccountPropertyChangedEvent):
        # Handle the properties_changed event

        # Fetch the Spark properties from event data
        props_string = event.spark_properties
        props = json.loads(props_string)

        # Create configuration file for app
        config_file = self._render_app_config_file(
            spark_properties=props
        )

        # Start application with rendered configuration
        self._start_application(config_file)

        # Set appropriate status
        self.unit.status = ActiveStatus("Spark service account properties changed")

    def _on_account_gone(self, event: ServiceAccountGoneEvent):
        # Handle the account_gone event

        # Create configuration file for app
        config_file = self._render_app_config_file(
            namespace=None,
            username=None,
            spark_properties=None
        )

        # Start application with rendered configuration
        self._start_application(config_file)

        # Set appropriate status
        self.unit.status = BlockedStatus("Missing spark service account")

### SparkServiceAccountProvider
Following is an example of using the SparkServiceAccountProvider class in the context
of the application charm code:

```python
from charms.data_platform_libs.v0.spark_service_account import (
    SparkServiceAccountProvider,
    ServiceAccountRequestedEvent,
    ServiceAccountReleasedEvent,
)


class ProviderCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        self.spark_service_account_provider = SparkServiceAccountProvider(self, relation_name="service-account")
        self.framework.observe(self.sa.on.account_requested, self._on_service_account_requested)
        self.framework.observe(self.sa.on.account_released, self._on_service_account_released)


    def _on_service_account_requested(self, event: ServiceAccountRequestedEvent):
        # Handle the account_requested event

        namespace, username = event.service_account.split(":")

        # Create the service account
        self.create_service_account(namespace, username)

        # Write the service account to relation data
        self.spark_service_account_provider.set_service_account(event.relation.id, f"{namespace}:{username}")

    def _on_service_account_released(self, event: ServiceAccountReleasedEvent):
        # Handle account_released event

        namespace, username = event.service_account.split(":")

        # Delete the service account
        self.delete_service_account(namespace, username)
```

"""


import logging
from typing import List, Optional

from ops import Model, RelationCreatedEvent, SecretChangedEvent
from ops.charm import (
    CharmBase,
    CharmEvents,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationEvent,
)
from ops.framework import EventSource, ObjectEvents

from charms.data_platform_libs.v0.data_interfaces import (
    SECRET_GROUPS,
    EventHandlers,
    ProviderData,
    RelationEventWithSecret,
    RequirerData,
    RequirerEventHandlers,
)

# The unique Charmhub library identifier, never change it
LIBID = "1f402a9b0ec547788b185c167ab9b5fe"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

PYDEPS = ["ops>=2.0.0"]

SPARK_PROPERTIES_RELATION_FIELD = "spark-properties"

logger = logging.getLogger(__name__)


class ServiceAccountEvent(RelationEventWithSecret):
    """Base class for Service account events."""

    @property
    def service_account(self) -> Optional[str]:
        """Returns the service account was requested."""
        if not self.relation.app:
            return None

        return self.relation.data[self.relation.app].get("service-account", "")

    @property
    def spark_properties(self) -> Optional[str]:
        """Returns the Spark properties associated with service account."""
        if not self.relation.app:
            return None

        if self.secrets_enabled:
            secret = self._get_secret("extra")
            if secret:
                return secret.get("spark-properties", "{}")

        return self.relation.data[self.relation.app].get("spark-properties", "{}")


class ServiceAccountRequestedEvent(ServiceAccountEvent):
    """Event emitted when a set of service account is requested for use on this relation."""


class ServiceAccountReleasedEvent(ServiceAccountEvent):
    """Event emitted when a set of service account is released."""


class SparkServiceAccountProviderEvents(CharmEvents):
    """Event descriptor for events raised by ServiceAccountProvider."""

    account_requested = EventSource(ServiceAccountRequestedEvent)
    account_released = EventSource(ServiceAccountReleasedEvent)


class ServiceAccountGrantedEvent(ServiceAccountEvent):
    """Event emitted when service account are granted on this relation."""


class ServiceAccountGoneEvent(RelationEvent):
    """Event emitted when service account are removed from this relation."""


class ServiceAccountPropertyChangedEvent(ServiceAccountEvent):
    """Event emitted when Spark properties for the service account are changed in this relation."""


class SparkServiceAccountRequirerEvents(ObjectEvents):
    """Event descriptor for events raised by the Requirer."""

    account_granted = EventSource(ServiceAccountGrantedEvent)
    account_gone = EventSource(ServiceAccountGoneEvent)
    properties_changed = EventSource(ServiceAccountPropertyChangedEvent)


class SparkServiceAccountProviderData(ProviderData):
    """Implementation of ProviderData for the Spark Service Account relation."""

    RESOURCE_FIELD = "service-account"

    def __init__(self, model: Model, relation_name: str) -> None:
        super().__init__(model, relation_name)

    def set_service_account(self, relation_id: int, service_account: str) -> None:
        """Set the service account name in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            service_account: the service account name.
        """
        self.update_relation_data(relation_id, {"service-account": service_account})

    def set_spark_properties(self, relation_id: int, spark_properties: str) -> None:
        """Set the Spark properties in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            spark_properties: the dictionary that contains key-value for Spark properties.
        """
        self.update_relation_data(relation_id, {SPARK_PROPERTIES_RELATION_FIELD: spark_properties})


class SparkServiceAccountProviderEventHandlers(EventHandlers):
    """Provider-side of the Spark Service Account relation."""

    on = SparkServiceAccountProviderEvents()  # pyright: ignore [reportAssignmentType]

    def __init__(self, charm: CharmBase, relation_data: SparkServiceAccountProviderData) -> None:
        super().__init__(charm, relation_data)
        # Just to keep lint quiet, can't resolve inheritance. The same happened in super().__init__() above
        self.relation_data = relation_data
        self.framework.observe(
            charm.on[self.relation_data.relation_name].relation_broken,
            self._on_relation_broken,
        )

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the relation has changed."""
        # Leader only
        if not self.relation_data.local_unit.is_leader():
            return

        diff = self._diff(event)
        # emit on account requested if service account name is provided by the requirer application
        if "service-account" in diff.added:
            getattr(self.on, "account_requested").emit(
                event.relation, app=event.app, unit=event.unit
            )

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """React to the relation broken event by releasing the service account."""
        # Leader only
        if not self.relation_data.local_unit.is_leader():
            return

        getattr(self.on, "account_released").emit(event.relation, app=event.app, unit=event.unit)


class SparkServiceAccountProvider(
    SparkServiceAccountProviderData, SparkServiceAccountProviderEventHandlers
):
    """Provider-side of the Spark Service Account relation."""

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        SparkServiceAccountProviderData.__init__(self, charm.model, relation_name)
        SparkServiceAccountProviderEventHandlers.__init__(self, charm, self)


class SparkServiceAccountRequirerData(RequirerData):
    """Implementation of RequirerData for the Spark Service Account relation."""

    def __init__(
        self,
        model: Model,
        relation_name: str,
        service_account: str,
        additional_secret_fields: Optional[List[str]] = [],
    ):
        """Manager of Spark Service Account relations."""
        if not additional_secret_fields:
            additional_secret_fields = []
        if SPARK_PROPERTIES_RELATION_FIELD not in additional_secret_fields:
            additional_secret_fields.append(SPARK_PROPERTIES_RELATION_FIELD)
        super().__init__(model, relation_name, additional_secret_fields=additional_secret_fields)
        self.service_account = service_account

    @property
    def service_account(self):
        """Service account used for Spark."""
        return self._service_account

    @service_account.setter
    def service_account(self, value):
        self._service_account = value


class SparkServiceAccountRequirerEventHandlers(RequirerEventHandlers):
    """Requirer-side event handlers of the Spark Service Account relation."""

    on = SparkServiceAccountRequirerEvents()  # pyright: ignore [reportAssignmentType]

    def __init__(self, charm: CharmBase, relation_data: SparkServiceAccountRequirerData) -> None:
        super().__init__(charm, relation_data)
        # Just to keep lint quiet, can't resolve inheritance. The same happened in super().__init__() above
        self.relation_data = relation_data
        self.framework.observe(
            charm.on[self.relation_data.relation_name].relation_broken,
            self._on_relation_broken,
        )

    def _on_relation_created_event(self, event: RelationCreatedEvent) -> None:
        """Event emitted when the Spark Service Account relation is created."""
        super()._on_relation_created_event(event)

        if not self.relation_data.local_unit.is_leader():
            return

        # Sets service_account in the relation
        relation_data = {
            f: getattr(self.relation_data, f.replace("-", "_"), "") for f in ["service-account"]
        }

        self.relation_data.update_relation_data(event.relation.id, relation_data)

    def _on_secret_changed_event(self, event: SecretChangedEvent):
        """Event notifying about a new value of a secret."""
        if not event.secret.label:
            return

        relation = self.relation_data._relation_from_secret_label(event.secret.label)
        if not relation:
            logging.info(
                f"Received secret {event.secret.label} but couldn't parse, seems irrelevant"
            )
            return

        if relation.app == self.charm.app:
            logging.info("Secret changed event ignored for Secret Owner")

        remote_unit = None
        for unit in relation.units:
            if unit.app != self.charm.app:
                remote_unit = unit

        getattr(self.on, "properties_changed").emit(relation, app=relation.app, unit=remote_unit)

    def _on_relation_changed_event(self, event: RelationChangedEvent) -> None:
        """Event emitted when the Spark Service Account relation has changed."""
        logger.info("On Spark Service Account relation changed")
        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Register all new secrets with their labels
        if any(newval for newval in diff.added if self.relation_data._is_secret_field(newval)):
            self.relation_data._register_secrets_to_relation(event.relation, diff.added)

        secret_field_user = self.relation_data._generate_secret_field_name(SECRET_GROUPS.USER)

        if ("service-account" in diff.added) or secret_field_user in diff.added:
            getattr(self.on, "account_granted").emit(
                event.relation, app=event.app, unit=event.unit
            )

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Notify the charm about a broken service account relation."""
        logger.info("On Spark Service Account relation gone")
        getattr(self.on, "account_gone").emit(event.relation, app=event.app, unit=event.unit)


class SparkServiceAccountRequirer(
    SparkServiceAccountRequirerData, SparkServiceAccountRequirerEventHandlers
):
    """Requirer side of the Spark Service Account relation."""

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
        service_account: str,
        additional_secret_fields: Optional[List[str]] = [],
    ) -> None:
        SparkServiceAccountRequirerData.__init__(
            self,
            charm.model,
            relation_name,
            service_account,
            additional_secret_fields,
        )
        SparkServiceAccountRequirerEventHandlers.__init__(self, charm, self)
