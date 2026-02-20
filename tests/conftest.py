#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
from platform import machine

import jubilant
import pytest
from jubilant import Juju

logger = logging.getLogger(__name__)


LXD_CONTROLLER = "lxd-controller"


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
