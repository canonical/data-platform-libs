#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to S3 provider charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import CharmBase, RelationJoinedEvent
from ops.main import main
from ops.model import ActiveStatus, WaitingStatus

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)

logger = logging.getLogger(__name__)
FIRST_RELATION = "first-s3-credentials"
SECOND_RELATION = "second-s3-credentials"


class ApplicationS3Charm(CharmBase):
    """Application charm that connects to s3 provider charms."""

    def __init__(self, *args):
        super().__init__(*args)

        # Default charm events.
        self.framework.observe(self.on.start, self._on_start)

        # Events related to the first s3 relation that is requested
        # (these events are defined in the s3 requires charm library).
        first_bucket_name = f'{self.app.name.replace("-", "_")}_first_bucket'
        self.first_s3_requirer = S3Requirer(self, FIRST_RELATION, first_bucket_name)
        self.framework.observe(
            self.first_s3_requirer.on.credentials_changed, self._on_first_credential_created
        )
        self.framework.observe(
            self.first_s3_requirer.on.credentials_gone, self._on_first_credential_gone
        )
        self.framework.observe(
            self.on[FIRST_RELATION].relation_joined, self._on_first_relation_joined
        )

        # Events related to the second s3 relation that is requested
        # (these events are defined in the s3 requires charm library).
        self.second_s3_requirer = S3Requirer(self, SECOND_RELATION)
        self.framework.observe(
            self.second_s3_requirer.on.credentials_changed, self._on_second_credential_created
        )
        self.framework.observe(
            self.second_s3_requirer.on.credentials_gone, self._on_second_credential_gone
        )
        self.framework.observe(
            self.on[SECOND_RELATION].relation_joined, self._on_second_relation_joined
        )

    def _on_first_relation_joined(self, _: RelationJoinedEvent):
        """On first s3 credential relation joined."""
        self.unit.status = ActiveStatus()

    def _on_second_relation_joined(self, _: RelationJoinedEvent):
        """On second s3 credential relation joined."""
        self.unit.status = ActiveStatus()

    def _on_start(self, _) -> None:
        """Only sets an waiting status."""
        # self.unit.status = WaitingStatus("Waiting for relation")
        self.unit.status = WaitingStatus("Waiting for relation")

    # First credential changed events observers.
    def _on_first_credential_created(self, event: CredentialsChangedEvent) -> None:
        """Event triggered when S3 credential was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"First s3 credentials: access-key: {event.access_key} secret-key: {event.secret_key} bucket: {event.bucket}"
        )
        self.unit.status = ActiveStatus("Received s3 credentials of the first relation")

    def _on_first_credential_gone(self, _: CredentialsGoneEvent) -> None:
        """Event triggered when the S3 credentials are gone."""
        logger.info("First relation s3 credentials are no more valid.")

    # Second credential changed events observers.
    def _on_second_credential_created(self, event: CredentialsChangedEvent) -> None:
        """Event triggered when S3 credential was created for this application."""
        # Retrieve the credentials using the charm library.
        logger.info(
            f"Second s3 credentials: access-key: {event.access_key} secret-key: {event.secret_key} bucket: {event.bucket}"
        )
        self.unit.status = ActiveStatus("Received s3 credentials of the second relation")

    def _on_second_credential_gone(self, _: CredentialsGoneEvent) -> None:
        """Event triggered when the S3 credentials are gone."""
        logger.info("Second relation s3 credentials are no more valid.")


if __name__ == "__main__":
    main(ApplicationS3Charm)
