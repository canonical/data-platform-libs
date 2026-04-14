#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import pytest
from jubilant_adapters import JujuFixture

from .helpers import get_leader_id, get_non_leader_id

logger = logging.getLogger(__name__)

APP_NAME = "secrets-test"


def test_deploy_charms(juju: JujuFixture, secrets_charm):
    """Deploy both charm to use in the tests."""
    juju.ext.model.deploy(secrets_charm, application_name=APP_NAME, num_units=2, series="jammy")
    juju.ext.model.wait_for_idle(apps=[APP_NAME], status="active", wait_for_exact_units=2)


def test_add_get_secret_app(juju: JujuFixture):
    """Test basic functionality of getting/setting cached secrets."""
    leader_id = get_leader_id(juju, APP_NAME)
    leader_name = f"{APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "add-secret",
        **{
            "label": "grandmas-apple-pie",
            "content": {"secret-ingredient": "cinnamon"},
            "scope": "app",
        },
    )
    action.wait()
    assert action.results["return-code"] == 0

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-secret", **{"label": "grandmas-apple-pie"}
    )
    action.wait()
    assert action.results.get("grandmas-apple-pie") == {"secret-ingredient": "cinnamon"}


@pytest.mark.log_errors_allowed(
    'cannot apply changes: creating secrets: secret with label "grandmas-apple-pie" already exists'
)
def test_secret_label_unique(juju: JujuFixture):
    """Test basic functionality of getting/setting cached secrets."""
    leader_id = get_leader_id(juju, APP_NAME)
    leader_name = f"{APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "add-secret",
        **{
            "label": "grandmas-apple-pie",
            "content": {"secret-ingredient": "cinnamon and grated walnut"},
            "scope": "unit",
        },
    )
    action.wait()


def test_add_get_secret_unit(juju: JujuFixture):
    unit_id = get_non_leader_id(juju, APP_NAME)
    unit_name = f"{APP_NAME}/{unit_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "add-secret",
        **{
            "label": "grandmas-cranberry-pie",
            "content": {"secret-ingredient": "cranberry jam added!"},
            "scope": "unit",
        },
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-secret", **{"label": "grandmas-cranberry-pie"}
    )
    action.wait()

    assert action.results.get("grandmas-cranberry-pie") == {
        "secret-ingredient": "cranberry jam added!"
    }


def test_set_secret(juju: JujuFixture):
    """Test basic functionality of getting/setting cached secrets."""
    leader_id = get_leader_id(juju, APP_NAME)
    leader_name = f"{APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "add-secret",
        **{
            "label": "auntie-susans-muffins",
            "content": {"secret-ingredient": "nutella"},
            "scope": "app",
        },
    )
    action.wait()

    action = juju.ext.model.units.get(leader_name).run_action(
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
    action.wait()

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-secret", **{"label": "auntie-susans-muffins"}
    )
    action.wait()
    assert action.results.get("auntie-susans-muffins") == {
        "secret-ingredient": "nutella and chocolate chips",
        "baking-time": "25mins sharp",
    }
