#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Etcd requires interface implementation using data interfaces v0."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import ops
from charmlibs.interfaces.tls_certificates import Certificate

from charms.data_platform_libs.v0.data_interfaces import (
    LIBPATCH as DATA_INTERFACES_VERSION,
)

if DATA_INTERFACES_VERSION > 44:
    from charms.data_platform_libs.v0.data_interfaces import (
        DatabaseEndpointsChangedEvent,
        EtcdReadyEvent,
    )
    from charms.data_platform_libs.v0.data_interfaces import EtcdRequires as EtcdRequiresV0Base

if TYPE_CHECKING:
    from charm import ApplicationCharm

ETCD_DATA_DIR = "/var/lib/application-charm/etcd"

logger = logging.getLogger(__name__)


def _get_common_name_from_chain(mtls_cert: str) -> str:
    """Get common name from chain."""
    raw_cas = [
        cert.strip() for cert in mtls_cert.split("-----END CERTIFICATE-----") if cert.strip()
    ]
    cert = raw_cas[0] + "\n-----END CERTIFICATE-----"
    return Certificate.from_string(cert).common_name


if DATA_INTERFACES_VERSION > 44:

    class EtcdRequiresV0(ops.framework.Object):
        """EtcdRequires implementation for data interfaces version 0."""

        def __init__(
            self,
            charm: "ApplicationCharm",
        ) -> None:
            super().__init__(charm, "requirer-etcd")
            self.charm = charm
            self.etcd_interface = EtcdRequiresV0Base(
                charm=self.charm,
                relation_name="etcd-client",
                prefix="/requirer-charm/",
                mtls_cert=self.raw_certificate,
            )

            self.charm.framework.observe(
                self.etcd_interface.on.endpoints_changed, self._on_endpoints_changed
            )
            self.charm.framework.observe(
                self.etcd_interface.on.etcd_ready, self._on_resource_created
            )

        def update_mtls_certs(self, cert: str) -> None:
            """Set the mtls cert in the relation data bag."""
            if not self.etcd_relation:
                return
            self.etcd_interface.set_mtls_cert(self.etcd_relation.id, cert)

        def update_requests_from_certs(self, certs: list[Certificate]) -> None:
            """Update the requests in the relation data bag from the assigned certificates."""
            if not self.etcd_relation:
                return

            self.etcd_interface.set_mtls_cert(self.etcd_relation.id, certs[0].raw)

        def _on_endpoints_changed(self, event: DatabaseEndpointsChangedEvent) -> None:
            """Handle etcd client relation data changed event."""
            logger.info("Endpoints changed: %s", event.endpoints)
            if not event.endpoints:
                logger.error("No endpoints available")
                return

        def _on_resource_created(self, event: EtcdReadyEvent) -> None:
            """Handle etcd ready event."""
            logger.info("etcd ready")
            if not event.tls_ca:
                logger.error("No server CA chain available")
                return
            if not event.username:
                logger.error("No username available")
                return
            Path(ETCD_DATA_DIR).mkdir(exist_ok=True)
            Path(f"{ETCD_DATA_DIR}/ca.pem").write_text(event.tls_ca)

        @property
        def etcd_relation(self) -> ops.Relation | None:
            """Return the etcd relation if present."""
            if not hasattr(self, "etcd_interface"):
                return None
            return self.etcd_interface.relations[0] if len(self.etcd_interface.relations) else None

        @property
        def etcd_uris(self) -> str | None:
            """Return the etcd uris."""
            if not self.etcd_relation:
                return None
            return self.etcd_interface.fetch_relation_field(self.etcd_relation.id, "uris")

        @property
        def credentials(self) -> dict[str, str | None] | None:
            """Return the etcd credentials."""
            if not self.etcd_relation:
                return None

            return {
                "username": self.etcd_interface.fetch_relation_field(
                    self.etcd_relation.id, "username"
                ),
                "uris": self.etcd_interface.fetch_relation_field(self.etcd_relation.id, "uris"),
                "endpoints": self.etcd_interface.fetch_relation_field(
                    self.etcd_relation.id, "endpoints"
                ),
                "version": self.etcd_interface.fetch_relation_field(
                    self.etcd_relation.id, "version"
                ),
                "tls-ca": self.etcd_interface.fetch_relation_field(
                    self.etcd_relation.id, "tls-ca"
                ),
            }

        @property
        def raw_certificate(self) -> str:
            """Return the raw certificate."""
            certs, _ = self.charm.certificates.get_assigned_certificates()
            if not certs:
                return ""
            return certs[0].ca.raw if self.charm.send_ca_option else certs[0].certificate.raw
