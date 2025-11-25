#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Etcd requires interface implementation using data interfaces v1."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import ops
from charms.tls_certificates_interface.v4.tls_certificates import Certificate

from charms.data_platform_libs.v1.data_interfaces import (
    DataContractV1,
    RequirerCommonModel,
    RequirerDataContractV1,
    ResourceCreatedEvent,
    ResourceEndpointsChangedEvent,
    ResourceProviderModel,
    ResourceRequirerEventHandler,
    build_model,
)

if TYPE_CHECKING:
    from charm import ETCD_SNAP_DIR, ApplicationCharm


logger = logging.getLogger(__name__)


def _get_common_name_from_chain(mtls_cert: str) -> str:
    """Get common name from chain."""
    raw_cas = [
        cert.strip() for cert in mtls_cert.split("-----END CERTIFICATE-----") if cert.strip()
    ]
    cert = raw_cas[0] + "\n-----END CERTIFICATE-----"
    return Certificate.from_string(cert).common_name


class EtcdRequiresV1(ops.framework.Object):
    """EtcdRequires implementation for data interfaces version 1."""

    def __init__(
        self,
        charm: "ApplicationCharm",
    ) -> None:
        super().__init__(charm, "requirer-etcd")
        self.charm = charm
        self.etcd_interface = ResourceRequirerEventHandler(
            self.charm,
            relation_name="etcd-client",
            requests=self.client_requests,
            response_model=ResourceProviderModel,
        )

        self.framework.observe(self.etcd_interface.on.endpoints_changed, self._on_endpoints_changed)
        self.framework.observe(self.etcd_interface.on.resource_created, self._on_resource_created)

    def update_mtls_certs(self, cert: str) -> None:
        """Set the mtls cert in the relation data bag."""
        if not self.etcd_relation:
            return
        local_model = self.etcd_relation_local_model
        local_model.requests[0].mtls_cert = cert
        self.etcd_interface.interface.write_model(self.etcd_relation.id, local_model)

    def update_requests_from_certs(self, certs: list[Certificate]) -> None:
        """Update the requests in the relation data bag from the assigned certificates."""
        if not self.etcd_relation:
            return
        local_model = self.etcd_relation_local_model

        request_common_names = {
            _get_common_name_from_chain(request.mtls_cert): request
            for request in local_model.requests
            if request.mtls_cert
        }

        requests_to_send = []
        for certificate in certs:
            cur_request = request_common_names.get(
                certificate.common_name,
                RequirerCommonModel(resource=f"/{certificate.common_name}/"),
            )

            cur_request.mtls_cert = certificate.raw
            requests_to_send.append(cur_request)

        local_model.requests = requests_to_send
        self.etcd_interface.interface.write_model(self.etcd_relation.id, local_model)

    def _on_endpoints_changed(self, event: ResourceEndpointsChangedEvent[ResourceProviderModel]) -> None:
        """Handle etcd client relation data changed event."""
        response = event.response
        logger.info("Endpoints changed: %s", response.endpoints)
        if not response.endpoints:
            logger.error("No endpoints available")

    def _on_resource_created(self, event: ResourceCreatedEvent[ResourceProviderModel]) -> None:
        """Handle resource created event."""
        logger.info("Resource created")
        response = event.response
        if not response.tls_ca:
            logger.error("No server CA chain available")
            return
        if not response.username:
            logger.error("No username available")
            return
        Path(ETCD_SNAP_DIR).mkdir(exist_ok=True)
        Path(f"{ETCD_SNAP_DIR}/ca.pem").write_text(response.tls_ca)

    @property
    def etcd_relation(self) -> ops.Relation | None:
        """Return the etcd relation if present."""
        if not hasattr(self, "etcd_interface"):
            return None
        return self.etcd_interface.relations[0] if len(self.etcd_interface.relations) else None

    @property
    def etcd_uris(self) -> str | None:
        """Return the etcd uris."""
        remote_responses = self.remote_responses
        if not remote_responses:
            return None
        remote_response = remote_responses[0]
        return remote_response.uris

    @property
    def etcd_relation_local_model(self) -> RequirerDataContractV1[RequirerCommonModel]:
        """Return the etcd relation local model."""
        if not self.etcd_relation:
            raise RuntimeError("etcd relation not found")
        return build_model(
            self.etcd_interface.interface.repository(self.etcd_relation.id),
            RequirerDataContractV1[RequirerCommonModel],
        )

    @property
    def remote_responses(self) -> list[ResourceProviderModel] | None:
        """Return the remote response model."""
        if not self.etcd_relation:
            return None

        return build_model(
            self.etcd_interface.interface.repository(
                self.etcd_relation.id, self.etcd_relation.app
            ),
            DataContractV1[ResourceProviderModel],
        ).requests

    @property
    def credentials(self) -> dict[str, str | None] | None:
        """Return the etcd credentials."""
        remote_responses = self.remote_responses
        if not remote_responses:
            return None
        remote_response = remote_responses[0]
        return {
            "username": ",".join([resp.username for resp in remote_responses if resp.username]),
            "uris": remote_response.uris if remote_response.uris else None,
            "endpoints": remote_response.endpoints,
            "version": remote_response.version,
            "tls-ca": remote_response.tls_ca if remote_response.tls_ca else None,
        }

    @property
    def client_requests(self) -> list:
        """Return the client requests for the etcd requirer interface."""
        return [
            RequirerCommonModel(
                resource=f"/{common_name}/",
                mtls_cert=self.charm.get_certificate_of_common_name(common_name) or "",
            )
            for common_name in self.charm.common_names
        ]
