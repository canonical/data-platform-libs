#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to etcd charm using data interfaces v0.

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

import ops
from charmlibs.interfaces.tls_certificates import (
    CertificateAvailableEvent,
    CertificateRequestAttributes,
    TLSCertificatesRequiresV4,
)
from etcd_requires import EtcdRequiresV0
from literals import CA_CERT_PATH, CLIENT_CERT_PATH, CLIENT_KEY_PATH, ETCD_DATA_DIR, ETCD_VERSION
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus
from workload import get, put

logger = logging.getLogger(__name__)


class ApplicationCharmEtcdClient(CharmBase):
    """Application charm that connects to etcd charm."""

    def __init__(self, *args):
        super().__init__(*args)

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
        self.framework.observe(self.on.update_mtls_certs_action, self._on_update_action)
        self.framework.observe(self.on.put_etcd_action, self._on_put_etcd_action)
        self.framework.observe(self.on.get_etcd_action, self._on_get_etcd_action)
        self.framework.observe(
            self.on.get_credentials_action, self._on_get_credentials_action_etcd
        )
        self.framework.observe(self.on.get_certificates_action, self._on_get_certificates_action)

    @property
    def common_name(self) -> str:
        """Return the common names for the client certificates."""
        return "requirer-charm"

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
        # workaround for snapd not being available on k8s charm: install etcdctl directly using wget
        try:
            self._install_etcdctl()
            logger.info("etcdctl installation completed successfully")
        except Exception as e:
            logger.error(f"Failed to install etcdctl: {e}", exc_info=True)
            event.defer()
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

        self.certificates.renew_certificate(certs[0])

        event.set_results({"message": "certificate renewed"})

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

        cert = certs[0]
        Path(ETCD_DATA_DIR).mkdir(parents=True, exist_ok=True)
        Path(CLIENT_CERT_PATH).write_text(cert.certificate.raw)
        Path(CLIENT_KEY_PATH).write_text(private_key.raw)
        key = (
            orig_key if orig_key.startswith("/") else f"/{cert.certificate.common_name}/{orig_key}"
        )
        result = put(uris, key, value)

        if not result:
            event.set_results(
                {
                    "ok": False,
                    "results": json.dumps(result),
                }
            )
            event.fail(
                f"etcdctl put failed for certificate with common name: {cert.certificate.common_name}"
            )
            return
        event.set_results(
            {
                "ok": True,
                "results": json.dumps(result),
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

        cert = certs[0]
        Path(ETCD_DATA_DIR).mkdir(parents=True, exist_ok=True)
        Path(CLIENT_CERT_PATH).write_text(cert.certificate.raw)
        Path(CLIENT_KEY_PATH).write_text(private_key.raw)
        key = (
            orig_key if orig_key.startswith("/") else f"/{cert.certificate.common_name}/{orig_key}"
        )

        result = get(uris, key)

        if not result:
            event.set_results(
                {
                    "ok": False,
                    "results": json.dumps(result),
                }
            )
            event.fail(
                f"etcdctl get failed for certificate with common name: {cert.certificate.common_name}"
            )
            return
        event.set_results(
            {
                "ok": True,
                "results": json.dumps(result),
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
            url = f"https://github.com/etcd-io/etcd/releases/download/v{ETCD_VERSION}/etcd-v{ETCD_VERSION}-linux-arm64.tar.gz"
        else:
            url = f"https://github.com/etcd-io/etcd/releases/download/v{ETCD_VERSION}/etcd-v{ETCD_VERSION}-linux-amd64.tar.gz"
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
            if entry.startswith(f"etcd-v{ETCD_VERSION}-linux-"):
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


if __name__ == "__main__":
    main(ApplicationCharmEtcdClient)
