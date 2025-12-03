#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import json
import logging
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import ops
from charmlibs import snap
from charmlibs.interfaces.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from etcd_requires import EtcdRequiresV1
from ops import Relation
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_fixed

from charms.data_platform_libs.v1.data_interfaces import (
    AuthenticationUpdatedEvent,
    ExtraSecretStr,
    KafkaRequestModel,
    KafkaResponseModel,
    RequirerCommonModel,
    RequirerDataContractV1,
    ResourceCreatedEvent,
    ResourceEndpointsChangedEvent,
    ResourceEntityCreatedEvent,
    ResourceProviderModel,
    ResourceRequirerEventHandler,
    StatusRaisedEvent,
    StatusResolvedEvent,
)

logger = logging.getLogger(__name__)

# Extra roles that this application needs when interacting with the database.
EXTRA_USER_ROLES = "SUPERUSER"
EXTRA_USER_ROLES_KAFKA = "producer,consumer"
EXTRA_USER_ROLES_OPENSEARCH = "admin,default"
CONSUMER_GROUP_PREFIX = "test-prefix"
BAD_URL = "http://badurl"
ETCD_SNAP_DIR = "/var/snap/charmed-etcd/common"
ETCD_SNAP_NAME = "charmed-etcd"


class ExtendedResponseModel(ResourceProviderModel):
    topsecret: ExtraSecretStr = Field(default=None)
    donttellanyone: ExtraSecretStr = Field(default=None)
    new_field: str | None = Field(default=None)
    new_field_req: str | None = Field(default=None)
    new_field2_req: str | None = Field(default=None)


class ExtendedRequirerCommonModel(RequirerCommonModel):
    new_field: str | None = Field(default=None)


class RefreshTLSCertificatesEvent(ops.EventBase):
    """Event for refreshing peer TLS certificates."""


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    refresh_tls_certificates_event = ops.EventSource(RefreshTLSCertificatesEvent)

    def __init__(self, *args):
        super().__init__(*args)

        # etcd snap for etcdctl usage

        if not self._ensure_snap_installed():
            logger.error("Failed to ensure snapd is installed")
        print(f"Checking...{shutil.which('snap')}")
        print(
            f"Still checking...{subprocess.run(['snap', 'version'], capture_output=True, text=True).stdout}"
        )
        print(f"Here isfile: {os.path.isfile('/usr/bin/snap')}")
        print(f"Here: {os.path.exists('/snap')}")
        self.etcd_snap = snap.SnapCache()[ETCD_SNAP_NAME]

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.get_plugin_status_action, self._on_get_plugin_status)

        # Events related to the first database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f"{self.app.name.replace('-', '_')}_first_database_db"
        self.first_database = ResourceRequirerEventHandler(
            charm=self,
            relation_name="first-database-db",
            requests=[
                RequirerCommonModel(resource=database_name, extra_user_roles=EXTRA_USER_ROLES)
            ],
            response_model=ExtendedResponseModel,
        )
        self.first_database_roles = ResourceRequirerEventHandler(
            self,
            "first-database-roles",
            requests=[
                RequirerCommonModel(
                    resource=database_name, entity_type="USER", extra_user_roles=EXTRA_USER_ROLES
                )
            ],
            response_model=ExtendedResponseModel,
        )
        self.framework.observe(
            self.first_database.on.resource_created, self._on_first_database_created
        )
        self.framework.observe(
            self.first_database.on.endpoints_changed, self._on_first_database_endpoints_changed
        )
        self.framework.observe(
            self.first_database_roles.on.resource_entity_created,
            self._on_first_database_entity_created,
        )
        self.framework.observe(
            self.first_database.on.authentication_updated,
            self._on_first_database_auth_updated,
        )

        # Events related to the second database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f"{self.app.name.replace('-', '_')}_second_database_db"

        # Keeping the charm backwards compatible, for upgrades testing
        self.second_database = ResourceRequirerEventHandler(
            charm=self,
            relation_name="second-database-db",
            requests=[
                ExtendedRequirerCommonModel(
                    resource=database_name,
                    extra_user_roles=EXTRA_USER_ROLES,
                    external_node_connectivity=True,
                )
            ],
            response_model=ExtendedResponseModel,
        )

        self.framework.observe(
            self.second_database.on.resource_created, self._on_second_database_created
        )
        self.framework.observe(
            self.second_database.on.endpoints_changed, self._on_second_database_endpoints_changed
        )

        # Multiple database clusters charm events (clusters/relations without alias).
        database_name = f"{self.app.name.replace('-', '_')}_multiple_database_clusters"
        self.database_clusters = ResourceRequirerEventHandler(
            charm=self,
            relation_name="multiple-database-clusters",
            requests=[
                RequirerCommonModel(resource=database_name, extra_user_roles=EXTRA_USER_ROLES)
            ],
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.database_clusters.on.resource_created, self._on_cluster_database_created
        )
        self.framework.observe(
            self.database_clusters.on.endpoints_changed,
            self._on_cluster_endpoints_changed,
        )

        # Multiple database clusters charm events (defined dynamically
        # in the database requires charm library, using the provided cluster/relation aliases).
        database_name = f"{self.app.name.replace('-', '_')}_aliased_multiple_database_clusters"
        cluster_aliases = ["cluster1", "cluster2"]  # Aliases for the multiple clusters/relations.
        self.aliased_database_clusters = ResourceRequirerEventHandler(
            charm=self,
            relation_name="aliased-multiple-database-clusters",
            requests=[
                RequirerCommonModel(resource=database_name, extra_user_roles=EXTRA_USER_ROLES)
            ],
            response_model=ResourceProviderModel,
            relation_aliases=cluster_aliases,
        )
        # Each database cluster will have its own events
        # with the name having the cluster/relation alias as the prefix.
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_resource_created,
            self._on_cluster1_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_endpoints_changed,
            self._on_cluster1_endpoints_changed,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_resource_created,
            self._on_cluster2_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_endpoints_changed,
            self._on_cluster2_endpoints_changed,
        )

        # Kafka events

        self.kafka = ResourceRequirerEventHandler(
            charm=self,
            relation_name="kafka-client-topic",
            requests=[
                KafkaRequestModel(
                    resource="test-topic",
                    extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                    consumer_group_prefix=CONSUMER_GROUP_PREFIX,
                )
            ],
            response_model=KafkaResponseModel,
        )

        self.kafka_split_pattern = ResourceRequirerEventHandler(
            self,
            relation_name="kafka-split-pattern-client",
            requests=[
                KafkaRequestModel(
                    resource="test-topic-split-pattern",
                    extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                    consumer_group_prefix=CONSUMER_GROUP_PREFIX,
                )
            ],
            response_model=KafkaResponseModel,
        )
        self.framework.observe(
            self.kafka_split_pattern.on.endpoints_changed,
            self._on_kafka_bootstrap_server_changed,
        )
        self.framework.observe(
            self.kafka_split_pattern.on.resource_created, self._on_kafka_topic_created
        )
        self.kafka_roles = ResourceRequirerEventHandler(
            charm=self,
            relation_name="kafka-client-roles",
            requests=[
                KafkaRequestModel(
                    resource="test-topic",
                    entity_type="USER",
                    extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                    consumer_group_prefix=CONSUMER_GROUP_PREFIX,
                )
            ],
            response_model=KafkaResponseModel,
        )
        self.framework.observe(
            self.kafka_roles.on.resource_entity_created,
            self._on_kafka_entity_created,
        )

        self.framework.observe(
            self.kafka.on.endpoints_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(self.kafka.on.resource_created, self._on_kafka_topic_created)

        # Kafka Connect events

        self.connect_source = ResourceRequirerEventHandler(
            self,
            "connect-source",
            requests=[RequirerCommonModel(resource="http://10.10.10.10:8000")],
            response_model=ResourceProviderModel,
        )
        self.connect_sink = ResourceRequirerEventHandler(
            self,
            "connect-sink",
            requests=[RequirerCommonModel(resource=BAD_URL)],
            response_model=ResourceProviderModel,
        )

        self.framework.observe(
            self.connect_source.on.resource_created, self._on_connect_integration_created
        )

        self.framework.observe(
            self.connect_source.on.endpoints_changed,
            self._on_connect_endpoints_changed,
        )

        # OpenSearch events

        self.opensearch = ResourceRequirerEventHandler(
            charm=self,
            relation_name="opensearch-client-index",
            requests=[
                RequirerCommonModel(
                    resource="test-index", extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH
                )
            ],
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.opensearch.on.resource_created, self._on_opensearch_index_created
        )

        self.framework.observe(
            self.opensearch.on.authentication_updated, self._on_opensearch_authentication_updated
        )

        self.opensearch_roles = ResourceRequirerEventHandler(
            charm=self,
            relation_name="opensearch-client-roles",
            requests=[
                RequirerCommonModel(
                    resource="test-index",
                    entity_type="USER",
                    extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH,
                )
            ],
            response_model=ResourceProviderModel,
        )
        self.framework.observe(
            self.opensearch_roles.on.resource_entity_created,
            self._on_opensearch_entity_created,
        )

        # TLS certificates (required to test etcd client).

        self.certificates = TLSCertificatesRequiresV4(
            self,
            "certificates",
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=common_name,
                    sans_ip=frozenset({socket.gethostbyname(socket.gethostname())}),
                    sans_dns=frozenset({self.unit.name, socket.gethostname()}),
                )
                for common_name in self.common_names
            ],
            refresh_events=[self.refresh_tls_certificates_event],
        )

        self.framework.observe(
            self.certificates.on.certificate_available, self._on_certificate_available
        )

        # Etcd events

        self.etcd = EtcdRequiresV1(self)

        # actions

        self.framework.observe(self.on.reset_unit_status_action, self._on_reset_unit_status)
        self.framework.observe(self.on.set_mtls_cert_action, self._on_set_mtls_cert)
        self.framework.observe(self.on.update_mtls_cert_action, self._on_update_action)
        self.framework.observe(self.on.put_etcd_action, self._on_put_etcd_action)
        self.framework.observe(self.on.get_etcd_action, self._on_get_etcd_action)
        self.framework.observe(self.on.get_credentials_action, self._on_get_credentials_action)
        self.framework.observe(self.on.get_certificate_action, self._on_get_certificate_action)

        # Get/set/delete fields on second-database relations
        self.framework.observe(self.on.get_relation_field_action, self._on_get_relation_field)
        self.framework.observe(
            self.on.get_relation_self_side_field_action, self._on_get_relation_self_side_field
        )
        self.framework.observe(self.on.set_relation_field_action, self._on_set_relation_field)
        self.framework.observe(
            self.on.delete_relation_field_action, self._on_delete_relation_field
        )
        self._relation_endpoints: list[ResourceRequirerEventHandler] = [
            self.first_database,
            self.second_database,
            self.database_clusters,
            self.aliased_database_clusters,
            self.kafka,
            self.kafka_roles,
            self.connect_source,
            self.connect_sink,
            self.opensearch,
            self.opensearch_roles,
        ]

        self.framework.observe(self.first_database.on.status_raised, self._on_status_raised)
        self.framework.observe(self.first_database.on.status_resolved, self._on_status_resolved)

        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(self.on.config_changed, self._on_certificate_available)

    def _on_update_status(self, event):
        logger.info("Update status")

    def _get_relation(self, relation_id: int) -> tuple[ResourceRequirerEventHandler, Relation]:
        """Retrieve a relation by ID, together with the corresponding endpoint object ('Requires')."""
        for source in self._relation_endpoints:
            for relation in source.relations:
                if relation.id == relation_id:
                    return (source, relation)
        raise ValueError(f"Invalid relation id {relation_id}")

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    # Generic relation actions
    def _on_get_relation_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        value = None
        model = source.interface.build_model(relation.id, component=relation.app)
        for request in model.requests:
            value = getattr(request, event.params["field"].replace("-", "_"))
        event.set_results({"value": value if value else ""})

    def _on_get_relation_self_side_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        value = None
        model = source.interface.build_model(
            relation.id, model=RequirerDataContractV1[ExtendedRequirerCommonModel]
        )
        for request in model.requests:
            value = getattr(request, event.params["field"].replace("-", "_"))
        event.set_results({"value": value if value else ""})

    def _on_set_relation_field(self, event: ActionEvent):
        """Set requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        model = source.interface.build_model(
            relation.id, model=RequirerDataContractV1[ExtendedRequirerCommonModel]
        )
        for request in model.requests:
            setattr(request, event.params["field"].replace("-", "_"), event.params["value"])
        source.interface.write_model(relation.id, model)

    def _on_delete_relation_field(self, event: ActionEvent):
        """Delete requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        model = source.interface.build_model(
            relation.id, model=RequirerDataContractV1[ExtendedRequirerCommonModel]
        )
        for request in model.requests:
            setattr(request, event.params["field"].replace("-", "_"), None)
        source.interface.write_model(relation.id, model)

    # First database events observers.
    def _on_first_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"first database credentials: {event.response.username} {event.response.password}"
        )
        self.unit.status = ActiveStatus("received database credentials of the first database")

    def _on_first_database_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"first database endpoints have been changed to: {event.response.endpoints}")

    def _on_first_database_entity_created(self, event: ResourceEntityCreatedEvent) -> None:
        """Event triggered when a database entity was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"first database entity credentials: {event.response.entity_name}")
        self.unit.status = ActiveStatus("received entity credentials of the first database")

    def _on_first_database_auth_updated(self, event: AuthenticationUpdatedEvent) -> None:
        """Event triggered when a database entity was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"first database tls credentials: {event.response.tls}")
        self.unit.status = ActiveStatus("first_database_authentication_updated")

    # Second database events observers.
    def _on_second_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"second database credentials: {event.response.username} {event.response.password}"
        )
        self.unit.status = ActiveStatus("received database credentials of the second database")

    def _on_second_database_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"second database endpoints have been changed to: {event.response.endpoints}")

    # Multiple database clusters events observers.
    def _on_cluster_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"cluster {event.relation.app.name} credentials: {event.response.username} {event.response.password}"
        )
        self.unit.status = ActiveStatus(
            f"received database credentials for cluster {event.relation.app.name}"
        )

    def _on_cluster_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(
            f"cluster {event.relation.app.name} endpoints have been changed to: {event.response.endpoints}"
        )

    # Multiple database clusters events observers (for aliased clusters/relations).
    def _on_cluster1_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster1 credentials: {event.response.username} {event.response.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster1")

    def _on_cluster1_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster1 endpoints have been changed to: {event.response.endpoints}")

    def _on_cluster2_database_created(self, event: ResourceCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster2 credentials: {event.response.username} {event.response.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster2")

    def _on_cluster2_endpoints_changed(self, event: ResourceEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster2 endpoints have been changed to: {event.response.endpoints}")

    def _on_get_plugin_status(self, event: ActionEvent) -> None:
        """Returns the PostgreSQL plugin status (enabled/disabled)."""
        plugin = event.params.get("plugin")
        if not plugin:
            event.fail("Please provide a plugin name")
            return

        plugin_status = (
            "enabled" if self.first_database.is_postgresql_plugin_enabled(plugin) else "disabled"
        )
        event.set_results({"plugin-status": plugin_status})

    def _on_kafka_bootstrap_server_changed(self, event: ResourceEndpointsChangedEvent):
        """Event triggered when a bootstrap server was changed for this application."""
        logger.info(
            f"On kafka boostrap-server changed: bootstrap-server: {event.response.endpoints}"
        )
        self.unit.status = ActiveStatus("kafka_bootstrap_server_changed")

    def _on_kafka_topic_created(self, _: ResourceCreatedEvent):
        """Event triggered when a topic was created for this application."""
        logger.info("On kafka topic created")
        self.unit.status = ActiveStatus("kafka_topic_created")

    def _on_kafka_entity_created(self, _: ResourceEntityCreatedEvent) -> None:
        """Event triggered when a topic entity was created for this application."""
        logger.info("On kafka entity created")
        self.unit.status = ActiveStatus("kafka_entity_created")

    def _on_connect_integration_created(self, _: ResourceCreatedEvent):
        """Event triggered when Kafka Connect integration credentials are created for this application."""
        self.unit.status = ActiveStatus("connect_integration_created")

    def _on_connect_endpoints_changed(self, _: ResourceEndpointsChangedEvent):
        """Event triggered when Kafka Connect REST endpoints change."""
        self.unit.status = ActiveStatus("connect_endpoints_changed")

    def _on_opensearch_index_created(self, _: ResourceCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch index created event fired")
        self.unit.status = ActiveStatus("opensearch_index_created")

    def _on_opensearch_entity_created(self, _: ResourceEntityCreatedEvent):
        """Event triggered when an index entity was created for this application."""
        logger.info("On opensearch entity created event fired")
        self.unit.status = ActiveStatus("opensearch_entity_created")

    def _on_opensearch_authentication_updated(self, _: ResourceCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch authentication_updated event fired")
        self.unit.status = ActiveStatus("opensearch_authentication_updated")

    def _on_reset_unit_status(self, _: ActionEvent):
        """Handle the reset of status message for the unit."""
        self.unit.status = ActiveStatus()

    def _on_set_mtls_cert(self, event: ActionEvent):
        """Sets the MTLS cert for the relation."""
        cmd = f'openssl req -new -newkey rsa:2048 -days 365 -nodes -subj "/CN={self.unit.name.replace("/", "-")}" -x509 -keyout client.key -out client.pem'
        subprocess.check_output(cmd, shell=True, universal_newlines=True)
        cert = open("./client.pem", "r").read()
        relation = self.model.get_relation("kafka-split-pattern-client")
        assert relation
        model = self.kafka_split_pattern.interface.build_model(
            relation.id, RequirerDataContractV1[KafkaRequestModel], component=self.app
        )
        for response in model.requests:
            response.mtls_cert = cert
        self.kafka_split_pattern.interface.write_model(relation.id, model)
        event.set_results({"mtls-cert": cert})

    def _on_status_raised(self, event: StatusRaisedEvent):
        logger.info(f"Status raised: {event.status}")
        self.unit.status = ActiveStatus(
            f"Active Statuses: {[s.code for s in event.active_statuses]}"
        )

    def _on_status_resolved(self, event: StatusResolvedEvent):
        logger.info(f"Status resolved: {event.status}")
        self.unit.status = ActiveStatus(
            f"Active Statuses: {[s.code for s in event.active_statuses]}"
        )

    # def _on_etcd_endpoints_changed(self, event: ResourceEndpointsChangedEvent[ResourceProviderModel],
    # ) -> None:
    #     """Handle etcd client relation data changed event."""
    #     response = event.response
    #     logger.info("Endpoints changed: %s", response.endpoints)
    #     if not response.endpoints:
    #         logger.error("No endpoints available")
    #
    # def _on_etcd_client_created(self, event: ResourceCreatedEvent[ResourceProviderModel]) -> None:
    #     """Handle resource created event."""
    #     logger.info("Resource created")
    #     response = event.response
    #     if not response.tls_ca:
    #         logger.error("No server CA chain available")
    #         return
    #     if not response.username:
    #         logger.error("No username available")
    #         return
    #     Path(ETCD_SNAP_DIR).mkdir(exist_ok=True)
    #     Path(f"{ETCD_SNAP_DIR}/ca.pem").write_text(response.tls_ca)

    @property
    def common_names(self) -> list[str]:
        """Return the common names for the client certificates."""
        return [
            "client1.requirer-charm",
            "client2.requirer-charm",
        ]

    @property
    def server_ca_chain(self) -> str | None:
        """Return the server CA chain."""
        try:
            ca_chain = Path(f"{ETCD_SNAP_DIR}/ca.pem").read_text().strip()
        except FileNotFoundError:
            return None
        return ca_chain

    @property
    def send_ca_option(self) -> bool:
        """Return True if the CA chain is available."""
        return bool(self.config.get("send-ca-cert", False))

    def _on_install(self, event: ops.InstallEvent) -> None:
        """Handle install event."""
        # install the etcd snap
        if not self._install_etcd_snap():
            self.unit.status = ops.BlockedStatus("Failed to install etcd snap")
            return

    def _on_update_action(self, event: ops.ActionEvent) -> None:
        """Handle update mtls certificate action."""
        # client relation
        if not self.etcd.etcd_relation:
            event.fail("etcd-client relation not found")
            return

        certs, _ = self.certificates.get_assigned_certificates()
        if not certs:
            event.fail("No certificates available")
            return

        for cert in certs:
            self.certificates.renew_certificate(cert)

        event.set_results({"message": "certificates renewed"})

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handle certificate available event."""
        logger.info("Certificate available")
        certs, private_key = self.certificates.get_assigned_certificates()
        if not certs or not private_key:
            logger.error("No certificates available")
            return

        if self.etcd.etcd_relation:
            self.etcd.update_requests_from_certs(
                [cert.ca if self.send_ca_option else cert.certificate for cert in certs]
            )

    def _on_config_changed(self) -> None:
        """Handle config changed event."""
        self.refresh_tls_certificates_event.emit()

    def _on_put_etcd_action(self, event: ops.ActionEvent) -> None:
        """Handle put action."""
        if not self.etcd.etcd_relation:
            event.fail("The action can be run only after relation is created.")
            event.set_results({"ok": False})
            return
        orig_key = str(event.params.get("key", ""))
        value = str(event.params.get("value", ""))
        if not orig_key or not value:
            event.fail("Both key and value parameters are required.")
            event.set_results({"ok": False})
            return

        uris = self.etcd.etcd_uris

        if not uris:
            event.fail("No uris available")
            event.set_results({"ok": False})
            return

        certs, private_key = self.certificates.get_assigned_certificates()
        if not certs or not private_key:
            event.fail("No certificates available")
            return

        results = {}
        for cert in certs:
            Path(ETCD_SNAP_DIR).mkdir(exist_ok=True)
            Path(f"{ETCD_SNAP_DIR}/client.pem").write_text(cert.certificate.raw)
            Path(f"{ETCD_SNAP_DIR}/client.key").write_text(private_key.raw)
            key = (
                orig_key
                if orig_key.startswith("/")
                else f"/{cert.certificate.common_name}/{orig_key}"
            )
            if result := _put(uris, key, value):
                results[cert.certificate.common_name] = result
            else:
                results[cert.certificate.common_name] = "Failed"

        for common_name, result in results.items():
            if result == "Failed":
                event.set_results(
                    {
                        "ok": False,
                        "results": json.dumps(results),
                    }
                )
                event.fail(f"etcdctl put failed for certificate with common name: {common_name}")
                return
        event.set_results(
            {
                "ok": True,
                "results": json.dumps(results),
            }
        )

    def _on_get_etcd_action(self, event: ops.ActionEvent) -> None:
        """Handle get action."""
        certs, private_key = self.certificates.get_assigned_certificates()
        if not certs or not private_key:
            event.fail("No certificates available")
            return

        if not self.etcd.etcd_relation:
            event.fail("The action can be run only after relation is created.")
            event.set_results({"ok": False})
            return

        orig_key = str(event.params.get("key", ""))
        if not orig_key:
            event.fail("Key parameter is required.")
            event.set_results({"ok": False})
            return

        uris = self.etcd.etcd_uris
        if not uris:
            event.fail("No uris available")
            event.set_results({"ok": False})
            return

        results = {}
        for cert in certs:
            Path(ETCD_SNAP_DIR).mkdir(exist_ok=True)
            Path(f"{ETCD_SNAP_DIR}/client.pem").write_text(cert.certificate.raw)
            Path(f"{ETCD_SNAP_DIR}/client.key").write_text(private_key.raw)
            key = (
                orig_key
                if orig_key.startswith("/")
                else f"/{cert.certificate.common_name}/{orig_key}"
            )
            if result := _get(uris, key):
                results[cert.certificate.common_name] = result
            else:
                results[cert.certificate.common_name] = "Failed"

        for common_name, result in results.items():
            if result == "Failed":
                event.set_results(
                    {
                        "ok": False,
                        "results": json.dumps(results),
                    }
                )
                event.fail(f"etcdctl get failed for certificate with common name: {common_name}")
                return
        event.set_results(
            {
                "ok": True,
                "results": json.dumps(results),
            }
        )

    def _on_get_credentials_action(self, event: ops.ActionEvent) -> None:
        """Return the credentials an action response."""
        if not self.server_ca_chain:
            event.fail(
                "The server CA chain is not available. Please wait for the server to provide it."
            )
            event.set_results({"ok": False})
            return

        if not self.etcd.etcd_relation:
            event.fail("The action can be run only after relation is created.")
            event.set_results({"ok": False})
            return

        if not (credentials := self.etcd.credentials):
            event.fail("No credentials available")
            event.set_results({"ok": False})
            return

        event.set_results(
            {
                "ok": True,
                **credentials,
            }
        )

    def _on_get_certificate_action(self, event: ops.ActionEvent) -> None:
        """Return the certificate an action response."""
        certs, _ = self.certificates.get_assigned_certificates()
        if not certs:
            event.fail("No certificates available")
            return

        certs_to_send = [
            cert.ca.raw if self.send_ca_option else cert.certificate.raw for cert in certs
        ]
        event.set_results(
            {
                "certificates": json.dumps(certs_to_send),
            }
        )

    def _ensure_snap_installed(self) -> bool:
        """Ensure snapd is installed on the system."""
        if shutil.which("snap"):
            return True

        try:
            # Install snapd using apt
            subprocess.run(["apt", "update"], check=True)
            subprocess.run(["apt", "install", "-y", "snapd"], check=True)
            # subprocess.run(['systemctl', 'enable', '--now', 'snapd.socket'], check=True)

            # Wait for snapd to be ready
            max_retries = 30
            for _ in range(max_retries):
                if shutil.which("snap"):
                    return True
                time.sleep(4)

            return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install snap: {e}")
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def _install_etcd_snap(self) -> bool:
        """Install the etcd snap."""
        try:
            self.etcd_snap.ensure(snap.SnapState.Present, channel="3.6/edge")
            self.etcd_snap.hold()
            return True
        except snap.SnapError as e:
            logger.error(str(e))
            return False

    def get_certificate_of_common_name(self, common_name: str) -> str | None:
        """Return the certificate for a given common name."""
        certs, _ = self.certificates.get_assigned_certificates()
        if not certs:
            return None
        for cert in certs:
            if cert.certificate.common_name == common_name:
                return cert.ca.raw if self.send_ca_option else cert.certificate.raw
        return None


def _put(endpoints: str, key: str, value: str) -> str | None:
    """Put a key value pair in etcd."""
    if (
        not Path(f"{ETCD_SNAP_DIR}/client.pem").exists()
        or not Path(f"{ETCD_SNAP_DIR}/client.key").exists()
        or not Path(f"{ETCD_SNAP_DIR}/ca.pem").exists()
    ):
        logger.error("No client certificates available")
        return None

    try:
        output = subprocess.check_output(
            [
                "charmed-etcd.etcdctl",
                "--endpoints",
                endpoints,
                "--cert",
                f"{ETCD_SNAP_DIR}/client.pem",
                "--key",
                f"{ETCD_SNAP_DIR}/client.key",
                "--cacert",
                f"{ETCD_SNAP_DIR}/ca.pem",
                "put",
                key,
                value,
            ],
        )
    except subprocess.CalledProcessError:
        logger.error("etcdctl put failed")
        return None

    return output.decode("utf-8").strip()


def _get(endpoints: str, key: str) -> str | None:
    """Get a key value pair from etcd."""
    if (
        not Path(f"{ETCD_SNAP_DIR}/client.pem").exists()
        or not Path(f"{ETCD_SNAP_DIR}/client.key").exists()
        or not Path(f"{ETCD_SNAP_DIR}/ca.pem").exists()
    ):
        logger.error("No client certificates available")
        return None

    try:
        output = subprocess.check_output(
            [
                "charmed-etcd.etcdctl",
                "--endpoints",
                endpoints,
                "--cert",
                f"{ETCD_SNAP_DIR}/client.pem",
                "--key",
                f"{ETCD_SNAP_DIR}/client.key",
                "--cacert",
                f"{ETCD_SNAP_DIR}/ca.pem",
                "get",
                key,
            ],
        )
    except subprocess.CalledProcessError:
        logger.error("etcdctl get failed")
        return None

    return output.decode("utf-8").strip()


if __name__ == "__main__":
    main(ApplicationCharm)
