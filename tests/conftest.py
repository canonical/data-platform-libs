#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import argparse
import json
import logging
from pathlib import Path
from platform import machine

import jubilant
import pytest
from jubilant import Juju
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


LXD_CONTROLLER = "lxd-controller"


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
    valid_combinations = [(0, "jammy"), (1, "focal"), (1, "noble")]
    if (config.option.build_bases_index, config.option.os_series) not in valid_combinations:
        raise argparse.ArgumentError(
            None, f"Only base index combinations {valid_combinations} are accepted."
        )


@pytest.fixture(scope="session")
def dp_libs_ubuntu_series(pytestconfig) -> str:
    if pytestconfig.option.os_series:
        return pytestconfig.option.os_series


@pytest.fixture(scope="module")
def ops_test(ops_test: OpsTest, pytestconfig) -> OpsTest:
    """Re-defining OpsTest.build_charm in a way that it takes CI caching and build parameters into account.

    Build parameters (for charms available for multiple OS versions) are considered both when building the
    charm, or when fetching pre-built, CI cached version of it.
    """
    _build_charm = ops_test.build_charm

    # Add bases_index option (indicating which OS version to use)
    # when building the charm within the scope of the test run
    async def build_charm(charm_path, bases_index: int = None) -> Path:
        if not bases_index and pytestconfig.option.build_bases_index is not None:
            bases_index = pytestconfig.option.build_bases_index

        logger.info(f"Building charm {charm_path} with base index {bases_index}")

        return await _build_charm(charm_path, bases_index=bases_index)

    ops_test.build_charm = build_charm
    return ops_test


@pytest.fixture(scope="module")
async def application_charm_v0(ops_test: OpsTest):
    """Build the application charm."""
    charm_path = "tests/v0/integration/application-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def application_charm_v1(ops_test: OpsTest):
    """Build the application charm."""
    charm_path = "tests/v1/integration/application-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="package")
def arch() -> str:
    """Fixture to provide the platform architecture for testing."""
    platforms = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }
    return platforms.get(machine(), "amd64")


@pytest.fixture(scope="module")
def juju():
    with jubilant.temp_model() as juju:
        juju.wait_timeout = 1000
        yield juju


@pytest.fixture(scope="module")
def lxd_cloud(juju: jubilant.Juju):
    clouds = json.loads(juju.cli("clouds", "--format", "json", include_model=False))
    for cloud, details in clouds.items():
        if "lxd" == details.get("type"):
            logger.info(f"Identified LXD cloud: {cloud}")
            yield cloud
            return

    logger.error("No LXD cloud found in Juju clouds. Available clouds: {clouds}")


@pytest.fixture(scope="module")
def lxd_controller(lxd_cloud: str, juju: Juju):
    controllers = json.loads(juju.cli("controllers", "--format", "json", include_model=False))
    logger.debug(f"Available controllers: {controllers}")
    for controller, details in controllers.get("controllers").items():
        if lxd_cloud == details.get("cloud"):
            logger.info(f"Identified LXD controller: {controller}")
            yield controller
            return

    logger.info(f"No controller with LXD cloud found. Available controllers: {controllers}")
    logger.info("Bootstrapping new LXD controller.")
    juju.bootstrap(lxd_cloud, controller=LXD_CONTROLLER)
    yield LXD_CONTROLLER

    juju.cli(
        "destroy-controller",
        LXD_CONTROLLER,
        "--destroy-all-models",
        "--destroy-storage",
        "--no-prompt",
        "--force",
        include_model=False,
    )


@pytest.fixture(scope="module")
def juju_lxd_model(juju: Juju, lxd_cloud: str, lxd_controller: str):
    clouds_known = juju.cli("list-clouds", "--controller", lxd_controller, include_model=False)
    logger.debug(f"Known clouds: {clouds_known}")
    with jubilant.temp_model(cloud=lxd_cloud, controller=lxd_controller) as juju_lxd:
        juju_lxd.wait_timeout = 1000
        yield juju_lxd
