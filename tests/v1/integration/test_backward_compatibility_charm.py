#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .helpers import PROV_SECRET_PREFIX, get_application_relation_data, get_juju_secret

APPLICATION_APP_NAME = "backward-app"
RELATION_NAME = "backward-database"
DATABASE_APP_NAME = "database"

APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME]

DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/v1/integration/database-charm/metadata.yaml").read_text()
)


@pytest.mark.abort_on_fail
@pytest.mark.log_errors_allowed
@pytest.mark.skip_if_deployed
async def test_deploy_charms(ops_test: OpsTest, backward_compatibility_charm, database_charm):
    """Deploy both charms (application and the testing kafka app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    await asyncio.gather(
        ops_test.model.deploy(
            backward_compatibility_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            database_charm,
            application_name=DATABASE_APP_NAME,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            num_units=1,
            series="jammy",
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[DATABASE_APP_NAME], status="active", wait_for_exact_units=1
    )
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="active", wait_for_exact_units=1
    )


@pytest.mark.abort_on_fail
async def test_backward_relation_with_charm_libraries_secrets(ops_test: OpsTest):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    rel = await ops_test.model.add_relation(
        DATABASE_APP_NAME, f"{APPLICATION_APP_NAME}:{RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "backward_database_created"

    # Get the requests
    secret_uri = (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
        )
        or ""
    )
    secret_data = await get_juju_secret(ops_test, secret_uri)
    username = secret_data["username"]
    password = secret_data["password"]
    endpoints = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "endpoints"
    )
    database = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "database"
    )

    assert username == f"relation_{rel.id}_None"
    assert len(password) == 16
    assert endpoints
    assert database == "bwclient"
