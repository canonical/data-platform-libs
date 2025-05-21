import pytest
from ops.charm import CharmBase
from ops.testing import Harness

from charms.data_platform_libs.v0.data_secrets import (
    CachedSecret,
    SecretCache,
    generate_secret_label,
)


class TestCharm(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)


@pytest.fixture
def harness() -> Harness:
    harness = Harness(TestCharm)
    harness.set_leader(True)
    harness.begin_with_initial_hooks()
    return harness


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("scope", [("app"), ("unit")])
def test_cached_secret_works(scope, harness):
    """Testing basic functionalities of the CachedSecret class."""
    secret = CachedSecret(harness.charm, "my-label")
    secret.add_secret(content={"rumour": "Community Movie on the way"}, scope=scope)
    real_secret = harness.charm.model.get_secret(label="my-label")

    assert real_secret.get_content() == secret.get_content()
    assert secret.meta.label == real_secret.label
    assert secret.get_info().__dict__ == real_secret.get_info().__dict__


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_cached_secret_is_cached(harness, mocker):
    """Testing if no more calls to Juju than planned."""
    secret = CachedSecret(harness.charm, "mylabel")
    secret.add_secret(content={"rumour": "Community Movie on the way"}, scope="app")
    patched_get = mocker.patch("ops.model.Model.get_secret")
    patched_get_content = patched_get.return_value.get_content

    secret2 = CachedSecret(harness.charm, "mylabel")
    secret2.get_info()
    secret2.get_content()
    secret2.set_content({"teaser": "Arcane Season II."})
    patched_get.assert_called_once()
    patched_get_content.assert_called_once()


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_secret_cache(harness):
    """Testing the SecretCache class."""
    cache = SecretCache(harness.charm)

    cache.add("label1", {"rumour": "Community Movie on the way"}, scope="app")
    cache.add("label2", {"teaser1": "Arcane Season II.", "teaser2": "Dune II."}, scope="app")

    assert cache.get("label1").get_content() == {"rumour": "Community Movie on the way"}
    assert cache.get("label2").get_content() == {
        "teaser1": "Arcane Season II.",
        "teaser2": "Dune II.",
    }


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_generate_secret_label(harness):
    """Testing generate_secret_label()."""
    assert generate_secret_label(harness.charm, "app") == "test-charm.app"
    assert generate_secret_label(harness.charm, "unit") == "test-charm.unit"
