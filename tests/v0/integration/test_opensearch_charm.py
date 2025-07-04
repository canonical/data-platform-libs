#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_application_relation_data, get_juju_secret

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "requirer-app"
OPENSEARCH_APP_NAME = "opensearch-test"
APP_NAMES = [APPLICATION_APP_NAME, OPENSEARCH_APP_NAME]
INDEX_RELATION_NAME = "opensearch-client-index"
ROLES_RELATION_NAME = "opensearch-client-roles"

PROV_SECRET_PREFIX = "secret-"


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, application_charm, opensearch_charm):
    """Deploy both charms (application and the testing opensearch app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm, application_name=APPLICATION_APP_NAME, num_units=1, series="jammy"
        ),
        ops_test.model.deploy(
            opensearch_charm, application_name=OPENSEARCH_APP_NAME, num_units=1, series="jammy"
        ),
    )
    await asyncio.gather(
        ops_test.model.wait_for_idle(
            apps=[OPENSEARCH_APP_NAME], status="active", wait_for_exact_units=1
        ),
        ops_test.model.wait_for_idle(
            apps=[APPLICATION_APP_NAME], status="active", wait_for_exact_units=1
        ),
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_without_juju_secrets")
async def test_opensearch_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of opensearch relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{INDEX_RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_index_created"
    # check if index access is granted
    for unit in ops_test.model.applications[OPENSEARCH_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "username"
    )
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "password"
    )
    endpoints = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "endpoints"
    )
    index = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "index"
    )

    assert username == "admin"
    assert password == "password"
    assert endpoints == "host1:port,host2:port"
    assert index == "test-index"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_opensearch_relation_with_charm_libraries_secrets(ops_test: OpsTest):
    """Test basic functionality of opensearch relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{INDEX_RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_index_created"
    # check if index access is granted
    for unit in ops_test.model.applications[OPENSEARCH_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    username = secret_content["username"]
    password = secret_content["password"]

    endpoints = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "endpoints"
    )
    index = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "index"
    )

    assert username == "admin"
    assert password == "password"
    assert endpoints == "host1:port,host2:port"
    assert index == "test-index"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_opensearch_relation_secret_changed(ops_test: OpsTest):
    """Test basic functionality of opensearch relation interface."""
    # Get current password
    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    password = secret_content["password"]
    # Change admin password
    unit_name = f"{OPENSEARCH_APP_NAME}/0"
    action = await ops_test.model.units.get(unit_name).run_action("change-admin-password")
    await action.wait()

    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, INDEX_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    new_password = secret_content["password"]
    assert password != new_password

    # check unit message to check if the index_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_authentication_updated"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_without_juju_secrets")
async def test_opensearch_roles_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of opensearch-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_role_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_role_created"
    # check if index role is created
    for unit in ops_test.model.applications[OPENSEARCH_APP_NAME].units:
        assert "created" in unit.workload_status_message

    rolename = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "role-name"
    )
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "role-password"
    )

    assert rolename == "admin"
    assert password == "password"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_opensearch_roles_relation_with_charm_libraries_secrets(ops_test: OpsTest):
    """Test basic functionality of opensearch relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_role_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_role_created"
    # check if index role is created
    for unit in ops_test.model.applications[OPENSEARCH_APP_NAME].units:
        assert "created" in unit.workload_status_message

    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, ROLES_RELATION_NAME, f"{PROV_SECRET_PREFIX}role"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    rolename = secret_content["role-name"]
    password = secret_content["role-password"]

    assert rolename == "admin"
    assert password == "password"
