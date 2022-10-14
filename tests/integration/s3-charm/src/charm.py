#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""S3 provider charm that accepts connections from application charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from charms.data_platform_libs.v0.s3 import CredentialRequestedEvent, S3Provider
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, MaintenanceStatus

logger = logging.getLogger(__name__)


class S3Charm(CharmBase):
    """S3 charm that accepts connections from application charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Charm events defined in the s3 provides charm library.
        self.s3_provider = S3Provider(self, relation_name="s3-credentials")
        self.framework.observe(
            self.s3_provider.on.credentials_requested, self._on_credential_requested
        )

    def _on_start(self, _) -> None:
        """Only sets an waiting status."""
        # self.unit.status = WaitingStatus("Waiting for relation")
        self.unit.status = ActiveStatus("HERE")

    def _on_credential_requested(self, event: CredentialRequestedEvent):
        self.unit.status = MaintenanceStatus("Creating credentials")
        # retrieve bucket name from the requirer side
        bucket = event.bucket

        relation_id = event.relation.id

        connection_info = {
            "access-key": "test-access-key",
            "secret-key": "test-secret-key",
            "bucket": f"{bucket}",
            "path": "/path/",
            "endpoint": "s3.amazonaws.com",
            "region": "us",
            "s3-uri-style": "",
            "storage-class": "cinder",
            "tls-ca-chain": [],
            "s3-api-version": "1.0",
            "attributes": ["a1", "a2", "a3"],
        }
        # set connection info in the databag relation
        self.s3_provider.update_connection_info(relation_id, connection_info)

        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(S3Charm)
