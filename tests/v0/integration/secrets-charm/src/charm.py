#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to database charms.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus

from charms.data_platform_libs.v0.data_secrets import SecretCache, generate_secret_label

logger = logging.getLogger(__name__)


class SecretsCharm(CharmBase):
    """Application charm that connects to database charms."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.start, self._on_start)

        self.framework.observe(self.on.add_secret_action, self._on_add_secret_action)
        self.framework.observe(self.on.set_secret_action, self._on_set_secret_action)
        self.framework.observe(self.on.get_secret_action, self._on_get_secret_action)
        self.secret_cache = SecretCache(self)

    def _on_start(self, _) -> None:
        """Only sets an Active status."""
        self.unit.status = ActiveStatus()

    def _on_add_secret_action(self, event: ActionEvent):
        label = event.params.get("label")
        if not label:
            label = generate_secret_label()
        content = event.params.get("content")
        scope = event.params.get("scope")
        self.secret_cache.add(label, content, scope)

    def _on_set_secret_action(self, event: ActionEvent):
        label = event.params.get("label")
        content = event.params.get("content")
        secret = self.secret_cache.get(label)
        secret.set_content(content)

    def _on_get_secret_action(self, event: ActionEvent):
        """Return the secrets stored in juju secrets backend."""
        label = event.params.get("label")
        event.set_results({label: self.secret_cache.get(label).get_content()})


if __name__ == "__main__":
    main(SecretsCharm)
