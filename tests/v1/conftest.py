# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import argparse
import os
from importlib.metadata import version
from unittest.mock import PropertyMock

import pytest
from ops import JujuVersion
from pytest_mock import MockerFixture


def pytest_addoption(parser):
    parser.addoption(
        "--os-series", help="Ubuntu series for dp libs charm (e.g. jammy)", default="jammy"
    )
    parser.addoption(
        "--build-bases-index",
        type=int,
        help="Index of charmcraft.yaml base that matches --os-series",
        default=0,
    )


def pytest_configure(config):
    if (config.option.os_series is None) ^ (config.option.build_bases_index is None):
        raise argparse.ArgumentError(
            None,
            "--os-series and --build-bases-index must be given together",
        )
    # Note: Update defaults whenever charmcraft.yaml is changed
    valid_combinations = [(0, "jammy"), (1, "noble")]
    if (config.option.build_bases_index, config.option.os_series) not in valid_combinations:
        raise argparse.ArgumentError(
            None, f"Only base index combinations {valid_combinations} are accepted."
        )


@pytest.fixture(autouse=True)
def juju_has_secrets(mocker: MockerFixture):
    """This fixture will force the usage of secrets whenever run on Juju 3.x.

    NOTE: This is needed, as normally JujuVersion is set to 0.0.0 in tests
    (i.e. not the real juju version)
    """
    if juju_version := os.environ.get("LIBJUJU_VERSION_SPECIFIER"):
        juju_version.replace("==", "")
        juju_version = juju_version[2:].split(".")[0]
    else:
        juju_version = version("juju")

    if juju_version < "3":
        mocker.patch.object(JujuVersion, "has_secrets", new_callable=PropertyMock).return_value = (
            False
        )
        return False
    else:
        mocker.patch.object(JujuVersion, "has_secrets", new_callable=PropertyMock).return_value = (
            True
        )
        return True


@pytest.fixture
def only_with_juju_secrets(juju_has_secrets):
    """Pretty way to skip Juju 3 tests."""
    if not juju_has_secrets:
        pytest.skip("Secrets test only applies on Juju 3.x")


@pytest.fixture
def only_without_juju_secrets(juju_has_secrets):
    """Pretty way to skip Juju 2-specific tests.

    Typically: to save CI time, when the same check were executed in a Juju 3-specific way already
    """
    if juju_has_secrets:
        pytest.skip("Skipping legacy secrets tests")
