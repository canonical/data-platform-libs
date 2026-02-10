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
import platform
import shutil
import socket
import subprocess
import urllib
from pathlib import Path
from typing import Optional, Tuple

import ops
from charmlibs.interfaces.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from ops import JujuVersion, Relation
from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus

from charms.data_platform_libs.v0.data_interfaces import (
    LIBPATCH as DATA_INTERFACES_VERSION,
)
from charms.data_platform_libs.v0.data_interfaces import (
    BootstrapServerChangedEvent,
    DatabaseCreatedEvent,
    DatabaseEndpointsChangedEvent,
    DatabaseRequires,
    IndexCreatedEvent,
    KafkaRequires,
    OpenSearchRequires,
    TopicCreatedEvent,
)

if DATA_INTERFACES_VERSION > 34:
    from charms.data_platform_libs.v0.data_interfaces import (
        KafkaRequirerData,
        KafkaRequirerEventHandlers,
    )

if DATA_INTERFACES_VERSION > 44:
    from etcd_requires import EtcdRequiresV0

if DATA_INTERFACES_VERSION > 49:
    from charms.data_platform_libs.v0.data_interfaces import (
        ENTITY_USER,
        DatabaseEntityCreatedEvent,
        IndexEntityCreatedEvent,
        TopicEntityCreatedEvent,
    )

if DATA_INTERFACES_VERSION > 52:
    from charms.data_platform_libs.v0.data_interfaces import (
        IntegrationCreatedEvent,
        IntegrationEndpointsChangedEvent,
        KafkaConnectRequires,
    )


if DATA_INTERFACES_VERSION > 54:
    from charms.data_platform_libs.v0.data_interfaces import (
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
ETCD_DATA_DIR = "/var/lib/application-charm/etcd"
CLIENT_CERT_PATH = f"{ETCD_DATA_DIR}/client.pem"
CLIENT_KEY_PATH = f"{ETCD_DATA_DIR}/client.key"
CA_CERT_PATH = f"{ETCD_DATA_DIR}/ca.pem"


class ApplicationCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.get_plugin_status_action, self._on_get_plugin_status)

        # Events related to the first database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f"{self.app.name.replace('-', '_')}_first_database_db"
        self.first_database = DatabaseRequires(
            charm=self,
            relation_name="first-database-db",
            database_name=database_name,
            extra_user_roles=EXTRA_USER_ROLES,
        )
        self.framework.observe(
            self.first_database.on.database_created, self._on_first_database_created
        )
        self.framework.observe(
            self.first_database.on.endpoints_changed, self._on_first_database_endpoints_changed
        )

        if DATA_INTERFACES_VERSION > 49:
            self.first_database_roles = DatabaseRequires(
                charm=self,
                relation_name="first-database-roles",
                database_name=database_name,
                entity_type=ENTITY_USER,
                extra_user_roles=EXTRA_USER_ROLES,
            )
            self.framework.observe(
                self.first_database_roles.on.database_entity_created,
                self._on_first_database_entity_created,
            )

        if DATA_INTERFACES_VERSION > 53 and JujuVersion.from_environ().has_secrets:
            self.first_database_username = DatabaseRequires(
                charm=self,
                relation_name="first-database-username",
                database_name=f"{database_name}_creds",
                requested_entity_name="testuser",
            )
            self.framework.observe(
                self.first_database_username.on.database_created, self._on_first_database_created
            )

        # Events related to the second database that is requested
        # (these events are defined in the database requires charm library).
        database_name = f"{self.app.name.replace('-', '_')}_second_database_db"

        # Keeping the charm backwards compatible, for upgrades testing
        if DATA_INTERFACES_VERSION > 17:
            self.second_database = DatabaseRequires(
                charm=self,
                relation_name="second-database-db",
                database_name=database_name,
                extra_user_roles=EXTRA_USER_ROLES,
                additional_secret_fields=["topsecret", "donttellanyone"],
                external_node_connectivity=True,
            )
        else:
            self.second_database = DatabaseRequires(
                charm=self,
                relation_name="second-database-db",
                database_name=database_name,
                extra_user_roles=EXTRA_USER_ROLES,
            )

        self.framework.observe(
            self.second_database.on.database_created, self._on_second_database_created
        )
        self.framework.observe(
            self.second_database.on.endpoints_changed, self._on_second_database_endpoints_changed
        )

        # Multiple database clusters charm events (clusters/relations without alias).
        database_name = f"{self.app.name.replace('-', '_')}_multiple_database_clusters"
        self.database_clusters = DatabaseRequires(
            charm=self,
            relation_name="multiple-database-clusters",
            database_name=database_name,
            extra_user_roles=EXTRA_USER_ROLES,
        )
        self.framework.observe(
            self.database_clusters.on.database_created, self._on_cluster_database_created
        )
        self.framework.observe(
            self.database_clusters.on.endpoints_changed,
            self._on_cluster_endpoints_changed,
        )
        if DATA_INTERFACES_VERSION > 55:
            self.database_prefixes = DatabaseRequires(
                charm=self,
                relation_name="database-with-prefix",
                database_name="testdb*",
                extra_user_roles=EXTRA_USER_ROLES,
                prefix_matching="all",
            )

        # Multiple database clusters charm events (defined dynamically
        # in the database requires charm library, using the provided cluster/relation aliases).
        database_name = f"{self.app.name.replace('-', '_')}_aliased_multiple_database_clusters"
        cluster_aliases = ["cluster1", "cluster2"]  # Aliases for the multiple clusters/relations.
        self.aliased_database_clusters = DatabaseRequires(
            charm=self,
            relation_name="aliased-multiple-database-clusters",
            database_name=database_name,
            extra_user_roles=EXTRA_USER_ROLES,
            relations_aliases=cluster_aliases,
        )
        # Each database cluster will have its own events
        # with the name having the cluster/relation alias as the prefix.
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_database_created,
            self._on_cluster1_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster1_endpoints_changed,
            self._on_cluster1_endpoints_changed,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_database_created,
            self._on_cluster2_database_created,
        )
        self.framework.observe(
            self.aliased_database_clusters.on.cluster2_endpoints_changed,
            self._on_cluster2_endpoints_changed,
        )

        # Kafka events

        self.kafka = KafkaRequires(
            charm=self,
            relation_name="kafka-client-topic",
            topic="test-topic",
            extra_user_roles=EXTRA_USER_ROLES_KAFKA,
            consumer_group_prefix=CONSUMER_GROUP_PREFIX,
        )

        if DATA_INTERFACES_VERSION > 34:
            self.kafka_requirer_interface = KafkaRequirerData(
                model=self.model,
                relation_name="kafka-split-pattern-client",
                topic="test-topic-split-pattern",
                extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                consumer_group_prefix=CONSUMER_GROUP_PREFIX,
            )
            self.kafka_split_pattern = KafkaRequirerEventHandlers(
                self, relation_data=self.kafka_requirer_interface
            )
            self.framework.observe(
                self.kafka_split_pattern.on.bootstrap_server_changed,
                self._on_kafka_bootstrap_server_changed,
            )
            self.framework.observe(
                self.kafka_split_pattern.on.topic_created, self._on_kafka_topic_created
            )

        if DATA_INTERFACES_VERSION > 49:
            self.kafka_roles = KafkaRequires(
                charm=self,
                relation_name="kafka-client-roles",
                topic="test-topic",
                entity_type=ENTITY_USER,
                extra_user_roles=EXTRA_USER_ROLES_KAFKA,
                consumer_group_prefix=CONSUMER_GROUP_PREFIX,
            )
            self.framework.observe(
                self.kafka_roles.on.topic_entity_created,
                self._on_kafka_entity_created,
            )

        self.framework.observe(
            self.kafka.on.bootstrap_server_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(self.kafka.on.topic_created, self._on_kafka_topic_created)

        # Kafka Connect events

        if DATA_INTERFACES_VERSION > 52:
            self.connect_source = KafkaConnectRequires(
                self, "connect-source", "http://10.10.10.10:8080"
            )

            self.connect_sink = KafkaConnectRequires(self, "connect-sink", BAD_URL)

            self.framework.observe(
                self.connect_source.on.integration_created, self._on_connect_integration_created
            )

            self.framework.observe(
                self.connect_source.on.integration_endpoints_changed,
                self._on_connect_endpoints_changed,
            )

        # OpenSearch events

        self.opensearch = OpenSearchRequires(
            charm=self,
            relation_name="opensearch-client-index",
            index="test-index",
            extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH,
        )
        self.framework.observe(self.opensearch.on.index_created, self._on_opensearch_index_created)
        self.framework.observe(
            self.opensearch.on.authentication_updated, self._on_opensearch_authentication_updated
        )

        if DATA_INTERFACES_VERSION > 49:
            self.opensearch_roles = OpenSearchRequires(
                charm=self,
                relation_name="opensearch-client-roles",
                index="test-index",
                entity_type=ENTITY_USER,
                extra_user_roles=EXTRA_USER_ROLES_OPENSEARCH,
            )
            self.framework.observe(
                self.opensearch_roles.on.index_entity_created,
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
        )

        self.framework.observe(
            self.certificates.on.certificate_available, self._on_certificate_available
        )

        # Etcd events

        if self.model.juju_version.has_secrets and DATA_INTERFACES_VERSION > 44:
            self.etcd = EtcdRequiresV0(self)
            self.framework.observe(self.on.update_mtls_certs_action, self._on_update_action)
            self.framework.observe(self.on.put_etcd_action, self._on_put_etcd_action)
            self.framework.observe(self.on.get_etcd_action, self._on_get_etcd_action)
            self.framework.observe(
                self.on.get_credentials_action, self._on_get_credentials_action_etcd
            )
            self.framework.observe(
                self.on.get_certificates_action, self._on_get_certificates_action
            )

        # Status/Error Propagation
        if DATA_INTERFACES_VERSION > 54:
            db = f"{self.app.name.replace('-', '_')}"
            self.requirer_with_status = DatabaseRequires(
                charm=self,
                relation_name="database-with-status",
                database_name=db,
            )
            self.framework.observe(
                self.requirer_with_status.on.status_raised, self._on_status_raised
            )
            self.framework.observe(
                self.requirer_with_status.on.status_resolved, self._on_status_resolved
            )

        # actions

        self.framework.observe(self.on.reset_unit_status_action, self._on_reset_unit_status)
        self.framework.observe(self.on.set_mtls_cert_action, self._on_set_mtls_cert)

        # Get/set/delete fields on second-database relations
        self.framework.observe(self.on.get_relation_field_action, self._on_get_relation_field)
        self.framework.observe(
            self.on.get_relation_self_side_field_action, self._on_get_relation_self_side_field
        )
        self.framework.observe(self.on.set_relation_field_action, self._on_set_relation_field)
        self.framework.observe(
            self.on.delete_relation_field_action, self._on_delete_relation_field
        )
        self._relation_endpoints = [
            self.first_database,
            self.second_database,
            self.database_clusters,
            self.aliased_database_clusters,
            self.kafka,
            self.opensearch,
        ]

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
            ca_chain = Path(CA_CERT_PATH).read_text().strip()
        except FileNotFoundError:
            return None
        return ca_chain

    @property
    def send_ca_option(self) -> bool:
        """Return True if the CA chain is available."""
        return bool(self.config.get("send-ca-cert", False))

    def _get_relation(self, relation_id: int) -> Optional[Tuple[CharmBase, Relation]]:
        """Retrieve a relation by ID, together with the corresponding endpoint object ('Requires')."""
        for source in self._relation_endpoints:
            for relation in source.relations:
                if relation.id == relation_id:
                    return (source, relation)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    # Generic relation actions
    def _on_get_relation_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            value = source.fetch_relation_field(relation.id, event.params["field"])
        else:
            value = relation.data[relation.app].get(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_get_relation_self_side_field(self, event: ActionEvent):
        """Get requested relation field (OTHER side)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            value = source.fetch_my_relation_field(relation.id, event.params["field"])
        else:
            value = relation.data[source.local_app].get(event.params["field"])
        event.set_results({"value": value if value else ""})

    def _on_set_relation_field(self, event: ActionEvent):
        """Set requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            source.update_relation_data(
                relation.id, {event.params["field"]: event.params["value"]}
            )
        else:
            relation.data[source.local_app][event.params["field"]] = event.params["value"]

    def _on_delete_relation_field(self, event: ActionEvent):
        """Delete requested relation field on self-side (that's the only one writeable)."""
        source, relation = self._get_relation(event.params["relation_id"])
        if DATA_INTERFACES_VERSION > 17:
            source.delete_relation_data(relation.id, [event.params["field"]])
        else:
            del relation.data[source.local_app][event.params["field"]]

    # First database events observers.
    def _on_first_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"first database credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials of the first database")

    def _on_first_database_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"first database endpoints have been changed to: {event.endpoints}")

    if DATA_INTERFACES_VERSION > 49:

        def _on_first_database_entity_created(self, event: DatabaseEntityCreatedEvent) -> None:
            """Event triggered when a database entity was created for this application."""
            # Retrieve the credentials using the charm library.
            logger.info(f"first database entity credentials: {event.entity_name}")
            self.unit.status = ActiveStatus("received entity credentials of the first database")

    # Second database events observers.
    def _on_second_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"second database credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials of the second database")

    def _on_second_database_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"second database endpoints have been changed to: {event.endpoints}")

    # Multiple database clusters events observers.
    def _on_cluster_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"cluster {event.relation.app.name} credentials: {event.username} {event.password}"
        )
        self.unit.status = ActiveStatus(
            f"received database credentials for cluster {event.relation.app.name}"
        )

    def _on_cluster_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(
            f"cluster {event.relation.app.name} endpoints have been changed to: {event.endpoints}"
        )

    # Multiple database clusters events observers (for aliased clusters/relations).
    def _on_cluster1_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster1 credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster1")

    def _on_cluster1_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster1 endpoints have been changed to: {event.endpoints}")

    def _on_cluster2_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(f"cluster2 credentials: {event.username} {event.password}")
        self.unit.status = ActiveStatus("received database credentials for cluster2")

    def _on_cluster2_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
        """Event triggered when the read/write endpoints of the database change."""
        logger.info(f"cluster2 endpoints have been changed to: {event.endpoints}")

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

    def _on_kafka_bootstrap_server_changed(self, event: BootstrapServerChangedEvent):
        """Event triggered when a bootstrap server was changed for this application."""
        logger.info(
            f"On kafka boostrap-server changed: bootstrap-server: {event.bootstrap_server}"
        )
        self.unit.status = ActiveStatus("kafka_bootstrap_server_changed")

    def _on_kafka_topic_created(self, _: TopicCreatedEvent):
        """Event triggered when a topic was created for this application."""
        logger.info("On kafka topic created")
        self.unit.status = ActiveStatus("kafka_topic_created")

    if DATA_INTERFACES_VERSION > 49:

        def _on_kafka_entity_created(self, _: TopicEntityCreatedEvent) -> None:
            """Event triggered when a topic entity was created for this application."""
            logger.info("On kafka entity created")
            self.unit.status = ActiveStatus("kafka_entity_created")

    if DATA_INTERFACES_VERSION > 52:

        def _on_connect_integration_created(self, _: IntegrationCreatedEvent):
            """Event triggered when Kafka Connect integration credentials are created for this application."""
            self.unit.status = ActiveStatus("connect_integration_created")

        def _on_connect_endpoints_changed(self, _: IntegrationEndpointsChangedEvent):
            """Event triggered when Kafka Connect REST endpoints change."""
            self.unit.status = ActiveStatus("connect_endpoints_changed")

    def _on_opensearch_index_created(self, _: IndexCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch index created event fired")
        self.unit.status = ActiveStatus("opensearch_index_created")

    if DATA_INTERFACES_VERSION > 49:

        def _on_opensearch_entity_created(self, _: IndexEntityCreatedEvent):
            """Event triggered when an index entity was created for this application."""
            logger.info("On opensearch entity created event fired")
            self.unit.status = ActiveStatus("opensearch_entity_created")

    def _on_opensearch_authentication_updated(self, _: IndexCreatedEvent):
        """Event triggered when an index was created for this application."""
        logger.info("On opensearch authentication_updated event fired")
        self.unit.status = ActiveStatus("opensearch_authentication_updated")

    def _on_reset_unit_status(self, _: ActionEvent):
        """Handle the reset of status message for the unit."""
        self.unit.status = ActiveStatus()

    def _on_set_mtls_cert(self, event: ActionEvent):
        """Sets the MTLS cert for the relation."""
        if DATA_INTERFACES_VERSION < 47:
            event.fail("This action is not supported on lib version < 0.47.")
            return

        cmd = f'openssl req -new -newkey rsa:2048 -days 365 -nodes -subj "/CN={self.unit.name.replace("/", "-")}" -x509 -keyout client.key -out client.pem'
        subprocess.check_output(cmd, shell=True, universal_newlines=True)
        cert = open("./client.pem", "r").read()
        relation = self.model.get_relation("kafka-split-pattern-client")
        self.kafka_requirer_interface.set_mtls_cert(relation.id, cert)
        event.set_results({"mtls-cert": cert})

    # Status/Error Propagation
    if DATA_INTERFACES_VERSION > 54:

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

    def _on_install(self, event: ops.InstallEvent) -> None:
        """Handle install event."""
        # workaround for snapd not being available on k8s charm: install etcdctl directly using wget
        self._install_etcdctl()

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
            Path(ETCD_DATA_DIR).mkdir(parents=True, exist_ok=True)
            Path(CLIENT_CERT_PATH).write_text(cert.certificate.raw)
            Path(CLIENT_KEY_PATH).write_text(private_key.raw)
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
            Path(ETCD_DATA_DIR).mkdir(parents=True, exist_ok=True)
            Path(CLIENT_CERT_PATH).write_text(cert.certificate.raw)
            Path(CLIENT_KEY_PATH).write_text(private_key.raw)
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

    def _on_get_credentials_action_etcd(self, event: ops.ActionEvent) -> None:
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

    def _on_get_certificates_action(self, event: ops.ActionEvent) -> None:
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

    def _install_etcdctl(self):
        """Install etcdctl using Python urllib and tar, accounting for architecture."""
        etcdctl_path = "/usr/local/bin/etcdctl"
        if shutil.which("etcdctl"):
            logger.info("etcdctl already installed.")
            return
        logger.info("Installing etcdctl via Python urllib...")
        arch = platform.machine()
        if arch == "aarch64":
            url = "https://github.com/etcd-io/etcd/releases/download/v3.4.35/etcd-v3.4.35-linux-arm64.tar.gz"
        else:
            url = "https://github.com/etcd-io/etcd/releases/download/v3.4.35/etcd-v3.4.35-linux-amd64.tar.gz"
        tmp_dir = "/tmp/etcd_install"
        os.makedirs(tmp_dir, exist_ok=True)
        tar_path = os.path.join(tmp_dir, "etcd.tar.gz")
        # Download tarball using urllib
        try:
            with urllib.request.urlopen(url) as response, open(tar_path, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
        except Exception as e:
            logger.error(f"Failed to download etcdctl tarball: {e}")
            return
        # Extract tarball
        try:
            subprocess.run(["tar", "-xvf", tar_path, "-C", tmp_dir], check=True)
        except Exception as e:
            logger.error(f"Failed to extract etcdctl tarball: {e}")
            return
        # Find the extracted etcdctl binary
        for entry in os.listdir(tmp_dir):
            if entry.startswith("etcd-v3.4.35-linux-"):
                etcdctl_src = os.path.join(tmp_dir, entry, "etcdctl")
                if os.path.isfile(etcdctl_src):
                    try:
                        shutil.move(etcdctl_src, etcdctl_path)
                        os.chmod(etcdctl_path, 0o755)
                        logger.info(f"etcdctl installed at {etcdctl_path}")
                    except Exception as e:
                        logger.error(f"Failed to move etcdctl binary: {e}")
                    break
        else:
            logger.error("Failed to find etcdctl binary after extraction.")

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
        not Path(CLIENT_CERT_PATH).exists()
        or not Path(CLIENT_KEY_PATH).exists()
        or not Path(CA_CERT_PATH).exists()
    ):
        logger.error("No client certificates available")
        return None
    try:
        output = subprocess.check_output(
            [
                "etcdctl",
                "--endpoints",
                endpoints,
                "--cert",
                CLIENT_CERT_PATH,
                "--key",
                CLIENT_KEY_PATH,
                "--cacert",
                CA_CERT_PATH,
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
        not Path(CLIENT_CERT_PATH).exists()
        or not Path(CLIENT_KEY_PATH).exists()
        or not Path(CA_CERT_PATH).exists()
    ):
        logger.error("No client certificates available")
        return None
    try:
        output = subprocess.check_output(
            [
                "etcdctl",
                "--endpoints",
                endpoints,
                "--cert",
                CLIENT_CERT_PATH,
                "--key",
                CLIENT_KEY_PATH,
                "--cacert",
                CA_CERT_PATH,
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
