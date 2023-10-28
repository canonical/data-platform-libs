#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .helpers import (
    get_application_relation_data,
    get_leader_id,
)

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/integration/database-charm/metadata.yaml").read_text()
)
FIRST_DATABASE_RELATION_NAME = "first-database"
SECOND_DATABASE_RELATION_NAME = "second-database"

SECRET_REF_PREFIX = "secret-"


# Global variables to store the relation ID after creation
# def pytest_namespace():
#     return {"first_database_relation": None, "second_database_relation": None}


async def downgrade_to_databag(ops_test, app_name):
    for unit in ops_test.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        complete_command = (
            "scp tests/integration/data/data_interfaces.py "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/lib/charms/data_platform_libs/v0/"
        )
        x, stdout, y = await ops_test.juju(*complete_command.split())


async def upgrade_to_secrets(ops_test, app_name):
    for unit in ops_test.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        complete_command = (
            "scp lib/charms/data_platform_libs/v0/data_interfaces.py "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/lib/charms/data_platform_libs/v0/"
        )
        x, stdout, y = await ops_test.juju(*complete_command.split())


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, application_charm, database_charm):
    """Deploy both charms (application and database) to use in the tests."""
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm, application_name=APPLICATION_APP_NAME, num_units=1, series="jammy"
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
            series="jammy",
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME, DATABASE_APP_NAME], status="active", wait_for_exact_units=1
    )


#
# NOTE: Tests below have follow a strict sequence.
# Pls only insert among if strongly justified.
#

# -----------------------------------------------------
# Testing 'Requires - databag' vs. 'Provides - secrets'
# -----------------------------------------------------


@pytest.mark.abort_on_fail
async def test_unbalanced_versions_falling_back_to_databag_req_databag_vs_prov_secrets(
    ops_test: OpsTest,
):
    """Relating Requires (databag only) with Provides (that could use secrets).

    This should fall back to databag usage.
    """
    await downgrade_to_databag(ops_test, APPLICATION_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.first_database_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Secrest are correctly set in databag
    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "username"
    )
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "password"
    )

    assert username
    assert password

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    await action.wait()
    assert action.results.get("value") == username

    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password


@pytest.mark.abort_on_fail
async def test_transparent_upgrade_requires_databag_vs_provides_secrets(ops_test: OpsTest):
    """Upgrading Requires to use secrets (if possible).

    YET, already existing relations keep using the databag for all operations (fetch, update, delete)
    """
    await upgrade_to_secrets(ops_test, APPLICATION_APP_NAME)

    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
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

    # Relation data is correct
    assert uris_new_val == await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
    )
    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

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

    # Relation data is correct
    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "uris"
        )
        is None
    )

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert not action.results.get("value")


@pytest.mark.abort_on_fail
async def test_transparent_upgrade_requires_databag_vs_provides_secrets_upgrade_new_secret(
    ops_test: OpsTest,
):
    """After Requires upgrade, we stay on the databag."""
    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
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

    # We stay on the databag
    assert await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "tls-ca"
    )

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-tls"
        )
        is None
    )

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

    # Previously existing sensitive data stays where it was
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "password"
    )
    assert password

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password


# -----------------------------------------------------
# Testing 'Requires - secrets' vs. 'Provides - databag'
# -----------------------------------------------------


@pytest.mark.abort_on_fail
async def test_unbalanced_versions_falling_back_to_databag_req_secrets_vs_prov_databag(
    ops_test: OpsTest,
):
    """Relating Requires (secrets available) with Provides (databag only).

    This should fall back to databag usage.
    """
    await downgrade_to_databag(ops_test, DATABASE_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.second_database_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Relation data is correct
    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "username"
    )
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "password"
    )

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "username"},
    )
    await action.wait()
    assert action.results.get("value") == username

    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password


@pytest.mark.abort_on_fail
async def test_transparent_upgrade_keeping_databag_requires_secrets_vs_provides_databag(
    ops_test: OpsTest,
):
    """Upgrading Provides to use secrets (if possible).

    YET, already existing relations keep using the databag for all operations (fetch, update, delete)
    """
    await upgrade_to_secrets(ops_test, DATABASE_APP_NAME)

    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
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

    # Relation data is correct
    assert uris_new_val == await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "uris"
    )
    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

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

    # Relation data is correct
    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "uris"
        )
        is None
    )

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    await action.wait()
    assert not action.results.get("value")


@pytest.mark.abort_on_fail
async def test_transparent_upgrade_keeping_databag_requires_secrets_vs_provides_databag_upgrade_new_secret(
    ops_test: OpsTest,
):
    """After Provider upgrade, the relation remains on databag."""
    # Application charm leader
    leader_app_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
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

    tls_ca_val = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "tls-ca"
    )

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "secret-tls"
        )
        is None
    )

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

    # Previously existing sensitive data stays where it was
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "password"
    )
    assert password

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    action = await ops_test.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password

    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    await action.wait()
    assert action.results.get("value") == password
