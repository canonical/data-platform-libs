#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to etcd charm using data interfaces v0.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging
import socket
import subprocess
from pathlib import Path

import ops
from charmlibs import snap
from charmlibs.interfaces.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from etcd_requires import EtcdRequiresV0
from literals import CA_CERT_PATH, CLIENT_CERT_PATH, CLIENT_KEY_PATH, SNAP_DIR, SNAP_NAME
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)


class ApplicationCharmEtcdClient(CharmBase):
    """Application charm that connects to etcd charm."""

    def __init__(self, *args):
        super().__init__(*args)
        self.etcd_snap = snap.SnapCache()[SNAP_NAME]

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.install, self._on_install)

        # TLS certificates (required to test etcd client).

        self.certificates = TLSCertificatesRequiresV4(
            self,
            "certificates",
            certificate_requests=[
                CertificateRequestAttributes(
                    common_name=self.common_name,
                    sans_ip=frozenset({socket.gethostbyname(socket.gethostname())}),
                    sans_dns=frozenset({self.unit.name, socket.gethostname()}),
                )
            ],
        )
        self.framework.observe(
            self.certificates.on.certificate_available, self._on_certificate_available
        )

        # Etcd interface and related events
        self.etcd = EtcdRequiresV0(self)
        self.framework.observe(self.on.config_changed, self._on_certificate_available)
        self.framework.observe(self.on.update_mtls_cert_action, self._on_update_action)
        self.framework.observe(self.on.put_etcd_action, self._on_put_etcd_action)
        self.framework.observe(self.on.get_etcd_action, self._on_get_etcd_action)
        self.framework.observe(
            self.on.get_credentials_action, self._on_get_credentials_action_etcd
        )
        self.framework.observe(self.on.get_certificate_action, self._on_get_certificate_action)

    @property
    def common_name(self) -> str:
        """Return the common name for the client certificate."""
        return "requirer-charm"

    @property
    def ca_cert(self) -> str | None:
        """Return the CA certificate."""
        certs, _ = self.certificates.get_assigned_certificates()
        if not certs:
            return None
        return certs[0].ca.raw

    @property
    def raw_certificate(self) -> str | None:
        """Return the raw certificate."""
        if not hasattr(self, "certificates"):
            return None

        if self.send_ca_option:
            return self.ca_cert

        certs, _ = self.certificates.get_assigned_certificates()
        if certs:
            return certs[0].certificate.raw
        return ""

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

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

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
            event.fail("No certificate available")
            return

        self.certificates.renew_certificate(certs[0])
        event.set_results({"message": "certificate renewed"})

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handle certificate available event."""
        logger.info("Certificate available")
        certs, private_key = self.certificates.get_assigned_certificates()
        if not certs or not private_key:
            logger.error("No certificates available")
            return

        cert = certs[0]
        Path(SNAP_DIR).mkdir(exist_ok=True, parents=True)
        Path(CLIENT_CERT_PATH).write_text(cert.certificate.raw)
        Path(CLIENT_KEY_PATH).write_text(private_key.raw)

        if self.etcd.etcd_relation:
            self.etcd.set_mtls_cert(cert.certificate.raw)

    def _on_put_etcd_action(self, event: ops.ActionEvent) -> None:
        """Handle put action."""
        if not self.etcd.etcd_relation:
            event.fail("The action can be run only after relation is created.")
            event.set_results({"ok": False})
            return

        key = str(event.params.get("key", ""))
        value = str(event.params.get("value", ""))
        if not key or not value:
            event.fail("Both key and value parameters are required.")
            event.set_results({"ok": False})
            return

        uris = self.etcd.etcd_uris

        if not uris:
            event.fail("No uris available")
            event.set_results({"ok": False})
            return

        if result := _put(uris, key, value):
            event.set_results({"message": result})
        else:
            event.fail("etcdctl put failed")

    def _on_get_etcd_action(self, event: ops.ActionEvent) -> None:
        """Handle get action."""
        certs, _ = self.certificates.get_assigned_certificates()
        if not certs:
            event.fail("No certificates available")
            return

        if not self.etcd.etcd_relation:
            event.fail("The action can be run only after relation is created.")
            event.set_results({"ok": False})
            return

        uris = self.etcd.etcd_uris
        if not uris:
            event.fail("No uris available")
            event.set_results({"ok": False})
            return

        key = str(event.params.get("key", ""))
        if not key:
            event.fail("key parameter is required.")
            event.set_results({"ok": False})
            return

        result = _get(uris, key)
        if result:
            event.set_results({"message": result})
        else:
            event.fail("etcdctl get failed")

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

    def _on_get_certificate_action(self, event: ops.ActionEvent) -> None:
        """Return the certificate an action response."""
        if self.send_ca_option:
            event.set_results({"certificate": self.ca_cert})
        else:
            if not self.raw_certificate:
                event.fail("No certificates available")
                return
            event.set_results({"certificate": self.raw_certificate})

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
                "charmed-etcd.etcdctl",
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
        logger.error("etcdctl put failed", exc_info=True)
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
                "charmed-etcd.etcdctl",
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
        logger.error("etcdctl get failed", exc_info=True)
        return None
    return output.decode("utf-8").strip()


if __name__ == "__main__":
    main(ApplicationCharmEtcdClient)
