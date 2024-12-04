"""Component level tests."""

from unittest.mock import Mock

import pytest
from ops.charm import CharmBase
from ops.testing import Harness

from charms.data_platform_libs.v0.data_interfaces import CachedSecret


class EmptyCharm(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)


@pytest.fixture
def harness() -> Harness:
    harness = Harness(EmptyCharm)
    harness.set_leader(True)
    harness.begin_with_initial_hooks()
    return harness


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("scope", [("app"), ("unit")])
def test_cached_secret_no_update(scope, harness: Harness[EmptyCharm], monkeypatch):
    """Check that a cached secret do not trigger a revision if no update to content."""
    # Given
    content = {"value": "initialvalue"}
    secret = CachedSecret(harness.model, getattr(harness.charm, scope), "my-label")
    secret.add_secret(content)
    patched = Mock()
    # When
    with monkeypatch.context() as m:
        m.setattr(secret.meta, "set_content", patched)
        secret.set_content(content)

        secret.set_content({"value": "newvalue"})

    # Then
    patched.assert_called_once()
