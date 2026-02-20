#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Etcd requires interface implementation using data interfaces v0."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import ops
from charmlibs.interfaces.tls_certificates import Certificate
from literals import CA_CERT_PATH, ETCD_DATA_DIR

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseEndpointsChangedEvent,
    EtcdReadyEvent,
)
from charms.data_platform_libs.v0.data_interfaces import EtcdRequires as EtcdRequiresV0Base

if TYPE_CHECKING:
    from charm import ApplicationCharmEtcdClient

logger = logging.getLogger(__name__)


def _get_common_name_from_chain(mtls_cert: str) -> str:
    """Get common name from chain."""
    raw_cas = [
        cert.strip() for cert in mtls_cert.split("-----END CERTIFICATE-----") if cert.strip()
    ]
    cert = raw_cas[0] + "\n-----END CERTIFICATE-----"
    return Certificate.from_string(cert).common_name


class EtcdRequiresV0(ops.framework.Object):
    """EtcdRequires implementation for data interfaces version 0."""

    def __init__(
        self,
        charm: "ApplicationCharmEtcdClient",
    ) -> None:
        super().__init__(charm, "requirer-etcd")
        self.charm = charm
        self.etcd_interface = EtcdRequiresV0Base(
            charm=self.charm,
            relation_name="etcd-client",
            prefix="/requirer-charm/",
            mtls_cert=self.charm.raw_certificate,
        )

        self.charm.framework.observe(
            self.etcd_interface.on.endpoints_changed, self._on_endpoints_changed
        )
        self.charm.framework.observe(self.etcd_interface.on.etcd_ready, self._on_resource_created)

    def set_mtls_cert(self, cert: str) -> None:
        """Set the mtls cert in the relation data bag."""
        if not self.etcd_relation:
            return
        self.etcd_interface.set_mtls_cert(self.etcd_relation.id, cert)

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
        Path(ETCD_DATA_DIR).mkdir(parents=True, exist_ok=True)
        Path(CA_CERT_PATH).write_text(event.tls_ca)

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

        return self.etcd_interface.fetch_relation_data(
            relation_ids=[self.etcd_relation.id],
            fields=["username", "uris", "endpoints", "version", "tls-ca"],
        ).get(self.etcd_relation.id, {})
