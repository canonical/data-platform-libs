#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_charm

APPLICATION_NAME = "application"
APPLICATION_PATH = "tests/integration/application-charm"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_amd64")
async def test_arm_charm_on_amd_host(ops_test: OpsTest, dp_libs_ubuntu_series: str) -> None:
    """Tries deploying an arm64 charm on amd64 host."""
    await ops_test.model.deploy(
        await get_charm(APPLICATION_PATH, "arm64", 1),
        application_name=APPLICATION_NAME,
        num_units=1,
        series=dp_libs_ubuntu_series,
    )

    await ops_test.model.wait_for_idle(apps=[APPLICATION_NAME], status="blocked")


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_arm64")
async def test_amd_charm_on_arm_host(ops_test: OpsTest, dp_libs_ubuntu_series: str) -> None:
    """Tries deploying an amd64 charm on arm64 host."""
    await ops_test.model.deploy(
        await get_charm(APPLICATION_PATH, "amd64", 0),
        application_name=APPLICATION_NAME,
        num_units=1,
        series=dp_libs_ubuntu_series,
    )

    await ops_test.model.wait_for_idle(apps=[APPLICATION_NAME], status="blocked")
