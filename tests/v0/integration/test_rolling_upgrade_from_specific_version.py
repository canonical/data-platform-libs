#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from lib.charms.data_platform_libs.v0.data_interfaces import LIBPATCH
from pytest_operator.plugin import OpsTest

from .helpers import get_leader_id

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/v0/integration/database-charm/metadata.yaml").read_text()
)
DB_FIRST_DATABASE_RELATION_NAME = "first-database-db"
DB_SECOND_DATABASE_RELATION_NAME = "second-database-db"

SECRET_REF_PREFIX = "secret-"


def old_version_to_upgrade_from():
    """Determine how many versions to go back from tox environment (default: previous version)."""
    try:
        go_backwards = int(os.environ["TOX_ENV"].split("-")[-1])
    except TypeError:
        go_backwards = 1
    return LIBPATCH - go_backwards


async def downgrade_to_old_version(ops_test, app_name):
    """Helper function simulating a "rolling downgrade".

    The data_interfaces module is replaced "on-the-fly" by an older version.
    """
    version = old_version_to_upgrade_from()
    logger.info(f"Downgrading {app_name} to version {version}")
    for unit in ops_test.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        path = f"tests/v0/integration/data/data_interfaces.py.v{version}"

        result = subprocess.run(
            f"grep 'LIBPATCH = {version}' {path}",
            shell=True,
        )

        assert not result.returncode, "Incorrect version of data_interfaces fetched."

        complete_command = (
            f"scp {path} "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}"
            "/charm/lib/charms/data_platform_libs/v0/data_interfaces.py"
        )
        ret_code, stdout, _ = await ops_test.juju(*complete_command.split())
        # scp was successful
        assert not ret_code, f"Couldn't perform copy to {unit.name}."


async def upgrade_to_new_version(ops_test, app_name):
    """Helper function simulating a "rolling upgrade".

    The data_interfaces module is replaced "on-the-fly" by the latest version.
    """
    logger.info(f"Upgrading {app_name} to latest version")
    for unit in ops_test.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        complete_command = (
            "scp lib/charms/data_platform_libs/v0/data_interfaces.py "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/lib/charms/data_platform_libs/v0/"
        )
        ret_code, stdout, _ = await ops_test.juju(*complete_command.split())
        # scp was successful
        assert not ret_code, f"Couldn't perform copy to {unit.name}."


@pytest.mark.usefixtures("fetch_old_versions")
@pytest.mark.abort_on_fail
async def test_deploy_charms(
    ops_test: OpsTest, application_charm, database_charm, dp_libs_ubuntu_series
):
    """Deploy both charms (application and database) to use in the tests."""
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
        ops_test.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=DATABASE_APP_NAME,
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME, DATABASE_APP_NAME],
        status="active",
        wait_for_exact_units=1,
    )


# -----------------------------------------------------
# Testing 'Peer Relation'
# -----------------------------------------------------


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("component", ["app", "unit"])
async def test_peer_relation(component, ops_test: OpsTest):
    """Peer relation safe across upgrades."""
    await downgrade_to_old_version(ops_test, DATABASE_APP_NAME)

    # Setting and verifying two fields (one that should be a secret, one plain text)
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "monitor-password", "value": "blablabla"},
    )
    await action.wait()

    action = await ops_test.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "not-a-secret", "value": "plain text"},
    )
    await action.wait()

    action = await ops_test.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    await action.wait()
    assert action.results.get("value") == "blablabla"

    action = await ops_test.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    await action.wait()
    assert action.results.get("value") == "plain text"

    # Upgrade
    await upgrade_to_new_version(ops_test, DATABASE_APP_NAME)

    # Both secret and databag content can be modified -- even twice ;-)
    action = await ops_test.model.units.get(unit_name).run_action(
        "set-peer-relation-field-multiple",
        **{"component": component, "field": "monitor-password", "value": "blablabla_new"},
    )
    await action.wait()

    action = await ops_test.model.units.get(unit_name).run_action(
        "set-peer-relation-field-multiple",
        **{"component": component, "field": "not-a-secret", "value": "even more plain text"},
    )
    await action.wait()

    action = await ops_test.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    await action.wait()
    assert action.results.get("value") == "blablabla_new2"

    await upgrade_to_new_version(ops_test, DATABASE_APP_NAME)

    action = await ops_test.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    await action.wait()
    assert action.results.get("value") == "even more plain text2"

    # Removing both secret and databag content can be modified
    action = await ops_test.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    await action.wait()

    action = await ops_test.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    await action.wait()

    # ...successfully
    action = await ops_test.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    await action.wait()
    assert not action.results.get("value")

    action = await ops_test.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    await action.wait()
    assert not action.results.get("value")


#
# NOTE: Tests below have follow a strict sequence.
# Pls only insert among if strongly justified.
#

# -----------------------------------------------------
# Testing 'Requires - databag' vs. 'Provides - secrets'
# -----------------------------------------------------


@pytest.mark.abort_on_fail
async def test_unbalanced_versions_req_old_vs_prov_new(
    ops_test: OpsTest,
):
    """Relating Requires (old version) with Provides (latest)."""
    await downgrade_to_old_version(ops_test, APPLICATION_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.first_database_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Username
    leader_app_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    await action.wait()
    assert action.results.get("value")

    username = action.results.get("value")

    # Password
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value")

    password = action.results.get("value")

    # Username is correct
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    await action.wait()
    assert action.results.get("value") == username

    # Password is correct
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password


@pytest.mark.abort_on_fail
async def test_rolling_upgrade_requires_old_vs_provides_new(ops_test: OpsTest):
    """Upgrading Requires to the latest version of the libs."""
    await upgrade_to_new_version(ops_test, APPLICATION_APP_NAME)

    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-user')
    uris_new_val = "http://username:password@example.com"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.first_database_relation.id,
            "field": "uris",
            "value": uris_new_val,
        },
    )
    await action.wait()

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert action.results.get("value") == uris_new_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert action.results.get("value") == uris_new_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    await action.wait()

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert not action.results.get("value")


@pytest.mark.abort_on_fail
async def test_rolling_upgrade_requires_old_vs_provides_new_upgrade_new_secret(
    ops_test: OpsTest,
):
    """After Requires upgrade new secrets are possible to define."""
    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-tls')
    tls_ca_val = "<ca_cert_here>"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.first_database_relation.id,
            "field": "tls-ca",
            "value": tls_ca_val,
        },
    )
    await action.wait()

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()
    assert action.results.get("value") == tls_ca_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()
    assert action.results.get("value") == tls_ca_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()

    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()
    assert not action.results.get("value")

    # Username exists
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    await action.wait()
    assert action.results.get("value")

    # Password exists
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value")


# -----------------------------------------------------
# Testing 'Requires - secrets' vs. 'Provides - databag'
# -----------------------------------------------------


@pytest.mark.abort_on_fail
async def test_unbalanced_versions_req_new_vs_prov_old(
    ops_test: OpsTest,
):
    """Relating Requires (latest) with Provides (old version)."""
    await downgrade_to_old_version(ops_test, DATABASE_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.second_database_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Username exists
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "username"},
    )
    await action.wait()
    assert action.results.get("value")

    # Password exists
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value")


@pytest.mark.abort_on_fail
async def test_rolling_upgrade_requires_new_vs_provides_old(
    ops_test: OpsTest,
):
    """Upgrading Provides to latest version."""
    await upgrade_to_new_version(ops_test, DATABASE_APP_NAME)

    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-user')
    uris_new_val = "http://username:password@example.com"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "uris",
            "value": uris_new_val,
        },
    )
    await action.wait()

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert action.results.get("value") == uris_new_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert action.results.get("value") == uris_new_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    await action.wait()

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert not action.results.get("value")


@pytest.mark.abort_on_fail
async def test_rolling_upgrade_requires_new_vs_provides_old_upgrade_new_secret(
    ops_test: OpsTest,
):
    """After Provider upgrade, we can safely define new secrets."""
    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-tls')
    tls_ca_val = "<ca_cert_here>"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "tls-ca",
            "value": tls_ca_val,
        },
    )
    await action.wait()

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()
    assert action.results.get("value") == tls_ca_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()
    assert action.results.get("value") == tls_ca_val

    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()

    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    await action.wait()
    assert not action.results.get("value")

    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value")
    password = action.results.get("value")

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password
