#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Spark service account provider charm."""

import json
import logging

from ops import ActiveStatus
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from spark8t.domain import PropertyFile
import yaml

from charms.data_platform_libs.v0.data_interfaces import DataPeer
from charms.data_platform_libs.v0.spark_service_account import (
    ServiceAccountReleasedEvent,
    ServiceAccountRequestedEvent,
    SparkServiceAccountProvider,
)

logger = logging.getLogger(__name__)

NAMESPASCE = "default"
USERNAME = "user"
SERVICE_ACCOUNT = f"{NAMESPASCE}:{USERNAME}"
PEER_REL = "spark-properties"
PROVIDER_REL = "spark-service-account"
RESOURCE_MANIFEST = {"foo": "bar"}

class SparkServiceAccountProviderCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        self.peer_relation_data = DataPeer(self, relation_name=PEER_REL)

        # Charm events defined in the spark_service_account charm library.
        self.service_account_provider = SparkServiceAccountProvider(
            self, relation_name="spark-service-account"
        )

        self.framework.observe(
            self.service_account_provider.on.account_requested, self._on_service_account_requested
        )
        self.framework.observe(
            self.service_account_provider.on.account_released, self._on_service_account_released
        )
        self.framework.observe(
            self.on.get_spark_properties_action, self._on_get_spark_properties_action
        )
        self.framework.observe(
            self.on.add_spark_property_action, self._on_add_spark_property_action
        )
        self.framework.observe(self.on[PEER_REL].relation_changed, self._on_peer_relation_changed)

    def _on_start(self, _) -> None:
        """Only sets an active status."""
        self.unit.status = ActiveStatus("")
        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return
        self.peer_relation_data.update_relation_data(
            relation_id=peer_relation.id, data={"default-key": "default-val"}
        )

    def _create_service_account(self, service_account: str) -> None:
        logger.info(f"Created service account: {service_account}")

    def _delete_service_account(self, service_account: str) -> None:
        logger.info(f"Deleted service account: {service_account}")

    def _add_spark_property(self, property_line: str) -> None:
        key, val = PropertyFile.parse_property_line(property_line)
        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return
        self.peer_relation_data.update_relation_data(peer_relation.id, {key: val})
        logger.info(f"Added property {property_line}.")

    def _get_spark_properties(self) -> dict[str, str]:
        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return {}
        relation_data = self.peer_relation_data.fetch_my_relation_data([peer_relation.id])
        if not relation_data:
            return {}
        props = relation_data[peer_relation.id]
        return props

    def _on_get_spark_properties_action(self, event: ActionEvent) -> None:
        props = self._get_spark_properties()
        event.set_results({"spark-properties": json.dumps(props)})

    def _on_add_spark_property_action(self, event: ActionEvent) -> None:
        conf = event.params["conf"]
        self._add_spark_property(property_line=conf)
        event.set_results({"success": "true"})

    def _on_service_account_requested(self, event: ServiceAccountRequestedEvent):
        """Handle the `ServiceAccountRequested` event for the Spark Integration hub."""
        if not self.unit.is_leader() or not event.service_account:
            return
        self._create_service_account(event.service_account)
        properties = self._get_spark_properties()

        self.service_account_provider.set_service_account(event.relation.id, event.service_account)  # type: ignore
        self.service_account_provider.set_spark_properties(
            event.relation.id, json.dumps(properties)
        )
        self.service_account_provider.set_resource_manifest(event.relation.id, yaml.dump(RESOURCE_MANIFEST))  # type: ignore


    def _on_service_account_released(self, event: ServiceAccountReleasedEvent):
        """Handle the `ServiceAccountReleased` event for the Spark Integration hub."""
        if not self.unit.is_leader() or not event.service_account:
            return
        self._delete_service_account(event.service_account)

    def _on_peer_relation_changed(self, _):
        """Handle on PEER relation changed event."""
        provider_relation = self.model.get_relation(relation_name=PROVIDER_REL)
        if not provider_relation:
            return
        relation_id = provider_relation.id
        properties = self._get_spark_properties()
        self.service_account_provider.set_spark_properties(
            relation_id=relation_id, spark_properties=json.dumps(properties)
        )


if __name__ == "__main__":
    main(SparkServiceAccountProviderCharm)
