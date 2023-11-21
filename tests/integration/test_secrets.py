#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_leader_id, get_non_leader_id

logger = logging.getLogger(__name__)

APP_NAME = "secrets-test"


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, secrets_charm):
    """Deploy both charm to use in the tests."""
    await ops_test.model.deploy(
        secrets_charm, application_name=APP_NAME, num_units=2, series="jammy"
    )
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", wait_for_exact_units=2)


@pytest.mark.abort_on_fail
async def test_add_get_secret_app(ops_test: OpsTest):
    """Test basic functionality of getting/setting cached secrets."""
    leader_id = await get_leader_id(ops_test, APP_NAME)
    leader_name = f"{APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "add-secret",
        **{
            "label": "grandmas-apple-pie",
            "content": {"secret-ingredient": "cinnamon"},
            "scope": "app",
        },
    )
    await action.wait()
    assert action.results["return-code"] == 0

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-secret", **{"label": "grandmas-apple-pie"}
    )
    await action.wait()
    assert action.results.get("grandmas-apple-pie") == {"secret-ingredient": "cinnamon"}


@pytest.mark.abort_on_fail
@pytest.mark.log_errors_allowed(
    'cannot apply changes: creating secrets: secret with label "grandmas-apple-pie" already exists'
)
async def test_secret_label_unique(ops_test: OpsTest):
    """Test basic functionality of getting/setting cached secrets."""
    leader_id = await get_leader_id(ops_test, APP_NAME)
    leader_name = f"{APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "add-secret",
        **{
            "label": "grandmas-apple-pie",
            "content": {"secret-ingredient": "cinnamon and grated walnut"},
            "scope": "unit",
        },
    )
    await action.wait()


@pytest.mark.abort_on_fail
async def test_add_get_secret_unit(ops_test: OpsTest):
    unit_id = await get_non_leader_id(ops_test, APP_NAME)
    unit_name = f"{APP_NAME}/{unit_id}"
    action = await ops_test.model.units.get(unit_name).run_action(
        "add-secret",
        **{
            "label": "grandmas-cranberry-pie",
            "content": {"secret-ingredient": "cranberry jam added!"},
            "scope": "unit",
        },
    )
    await action.wait()

    action = await ops_test.model.units.get(unit_name).run_action(
        "get-secret", **{"label": "grandmas-cranberry-pie"}
    )
    await action.wait()

    assert action.results.get("grandmas-cranberry-pie") == {
        "secret-ingredient": "cranberry jam added!"
    }


@pytest.mark.abort_on_fail
async def test_set_secret(ops_test: OpsTest):
    """Test basic functionality of getting/setting cached secrets."""
    leader_id = await get_leader_id(ops_test, APP_NAME)
    leader_name = f"{APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "add-secret",
        **{
            "label": "auntie-susans-muffins",
            "content": {"secret-ingredient": "nutella"},
            "scope": "app",
        },
    )
    await action.wait()

    action = await ops_test.model.units.get(leader_name).run_action(
        "set-secret",
        **{
            "label": "auntie-susans-muffins",
            "content": {
                "secret-ingredient": "nutella and chocolate chips",
                "baking-time": "25mins sharp",
            },
            "scope": "app",
        },
    )
    await action.wait()

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-secret", **{"label": "auntie-susans-muffins"}
    )
    await action.wait()
    assert action.results.get("auntie-susans-muffins") == {
        "secret-ingredient": "nutella and chocolate chips",
        "baking-time": "25mins sharp",
    }
