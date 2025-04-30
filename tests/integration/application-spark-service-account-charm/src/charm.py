#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Spark service account requirer charm."""

import json
import logging

from ops import ActiveStatus, WaitingStatus
from ops.charm import ActionEvent, CharmBase
from ops.main import main

from charms.data_platform_libs.v0.data_interfaces import DataPeer
from charms.data_platform_libs.v0.spark_service_account import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    ServiceAccountPropertyChangedEvent,
    SparkServiceAccountRequirer,
)

logger = logging.getLogger(__name__)

NAMESPASCE = "default"
USERNAME1 = "user1"
SERVICE_ACCOUNT1 = f"{NAMESPASCE}:{USERNAME1}"
PEER_REL = "spark-properties"
REQUIRER_REL = "spark-service-account"


class SparkServiceAccountRequirerCharm(CharmBase):
    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        self.peer_relation_data = DataPeer(self, relation_name=PEER_REL)

        # Charm events defined in the spark_service_account charm library.
        self.service_account_requirer = SparkServiceAccountRequirer(
            self, relation_name=REQUIRER_REL, service_account=SERVICE_ACCOUNT1, skip_creation=False
        )

        self.framework.observe(
            self.service_account_requirer.on.account_granted, self._on_service_account_granted
        )
        self.framework.observe(
            self.service_account_requirer.on.account_gone, self._on_service_account_gone
        )
        self.framework.observe(
            self.service_account_requirer.on.properties_changed,
            self._on_service_account_properties_changed,
        )
        self.framework.observe(
            self.on.get_spark_properties_action, self._on_get_spark_properties_action
        )
        self.framework.observe(
            self.on.get_resource_manifest_action, self._on_get_resource_manifest_action
        )


    def _on_start(self, _) -> None:
        """Only sets a blocked status."""
        message = "Waiting for spark-service-account relation"
        if self.unit.is_leader():
            self.app.status = WaitingStatus(message)
        self.unit.status = WaitingStatus(message)

    def _on_service_account_granted(self, event: ServiceAccountGrantedEvent):
        """Handle the `ServiceAccountGranted` event."""
        if not self.unit.is_leader() or not event.service_account:
            return
        message = f"Service account granted: {event.service_account}"
        self.unit.status = ActiveStatus(message)
        self.app.status = ActiveStatus(message)
        spark_properties = json.loads(event.spark_properties or "{}")

        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return
        self.peer_relation_data.update_relation_data(
            relation_id=peer_relation.id, data=spark_properties
        )

    def _on_service_account_gone(self, event: ServiceAccountGoneEvent):
        """Handle the `ServiceAccountGone` event."""
        if not self.unit.is_leader():
            return
        message = "Waiting for spark-service-account relation"
        self.app.status = WaitingStatus(message)
        self.unit.status = WaitingStatus(message)

        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return
        peer_relation.data[self.app].clear()

    def _on_service_account_properties_changed(self, event: ServiceAccountPropertyChangedEvent):
        """Handle the `ServiceAccountPropertyChangedEvent`."""
        if not self.unit.is_leader() or not event.service_account:
            return

        requirer_relation = self.model.get_relation(relation_name=REQUIRER_REL)
        if not requirer_relation:
            return
        spark_properties = (
            self.service_account_requirer.fetch_relation_field(
                relation_id=requirer_relation.id, field="spark-properties"
            )
            or "{}"
        )
        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            return
        self.peer_relation_data.update_relation_data(
            relation_id=peer_relation.id, data=json.loads(spark_properties)
        )

    def _on_get_spark_properties_action(self, event: ActionEvent):
        peer_relation = self.model.get_relation(PEER_REL)
        if not peer_relation:
            logger.warning("No peer relation")
            return
        relation_data = self.peer_relation_data.fetch_my_relation_data([peer_relation.id])
        if not relation_data:
            return
        props = relation_data[peer_relation.id]
        event.set_results({"spark-properties": json.dumps(props)})


    def _on_get_resource_manifest_action(self, event: ActionEvent):
        sa_relation = self.model.get_relation(REQUIRER_REL)
        if not sa_relation:
            logger.warning("No service account relation")
            return
        
        resource_manifest = self.service_account_requirer.fetch_relation_field(
            relation_id=sa_relation.id, field="resource-manifest"
        )
        event.set_results({"resource-manifest": resource_manifest})


if __name__ == "__main__":
    main(SparkServiceAccountRequirerCharm)
