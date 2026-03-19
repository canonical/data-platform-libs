#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import pytest
from jubilant_adapters import JujuFixture, gather

from .helpers import get_application_relation_data, get_juju_secret

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "requirer-app"
OPENSEARCH_APP_NAME = "opensearch-test"
APP_NAMES = [APPLICATION_APP_NAME, OPENSEARCH_APP_NAME]
INDEX_RELATION_NAME = "opensearch-client-index"
ROLES_RELATION_NAME = "opensearch-client-roles"

PROV_SECRET_PREFIX = "secret-"


def test_deploy_charms(juju: JujuFixture, application_charm, opensearch_charm):
    """Deploy both charms (application and the testing opensearch app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    gather(
        juju.ext.model.deploy(
            application_charm, application_name=APPLICATION_APP_NAME, num_units=1, series="jammy"
        ),
        juju.ext.model.deploy(
            opensearch_charm, application_name=OPENSEARCH_APP_NAME, num_units=1, series="jammy"
        ),
    )
    gather(
        juju.ext.model.wait_for_idle(
            apps=[OPENSEARCH_APP_NAME], status="active", wait_for_exact_units=1
        ),
        juju.ext.model.wait_for_idle(
            apps=[APPLICATION_APP_NAME], status="active", wait_for_exact_units=1
        ),
    )


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_opensearch_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of opensearch relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{INDEX_RELATION_NAME}"
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_index_created"
    # check if index access is granted
    for unit in juju.ext.model.applications[OPENSEARCH_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    username = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "username"
    )
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "password"
    )
    endpoints = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "endpoints"
    )
    index = get_application_relation_data(juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "index")

    assert username == "admin"
    assert password == "password"
    assert endpoints == "host1:port,host2:port"
    assert index == "test-index"


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_opensearch_relation_with_charm_libraries_secrets(juju: JujuFixture):
    """Test basic functionality of opensearch relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{INDEX_RELATION_NAME}"
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_index_created"
    # check if index access is granted
    for unit in juju.ext.model.applications[OPENSEARCH_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    secret_uri = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = get_juju_secret(juju, secret_uri)
    username = secret_content["username"]
    password = secret_content["password"]

    endpoints = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "endpoints"
    )
    index = get_application_relation_data(juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, "index")

    assert username == "admin"
    assert password == "password"
    assert endpoints == "host1:port,host2:port"
    assert index == "test-index"


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_opensearch_relation_secret_changed(juju: JujuFixture):
    """Test basic functionality of opensearch relation interface."""
    # Get current password
    secret_uri = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = get_juju_secret(juju, secret_uri)
    password = secret_content["password"]
    # Change admin password
    unit_name = f"{OPENSEARCH_APP_NAME}/0"
    action = juju.ext.model.units.get(unit_name).run_action("change-admin-password")
    action.wait()

    secret_uri = get_application_relation_data(
        juju, APPLICATION_APP_NAME, INDEX_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = get_juju_secret(juju, secret_uri)
    new_password = secret_content["password"]
    assert password != new_password

    # check unit message to check if the index_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_authentication_updated"


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_opensearch_roles_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of opensearch-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}"
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_entity_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_entity_created"
    # check if index role is created
    for unit in juju.ext.model.applications[OPENSEARCH_APP_NAME].units:
        assert "created" in unit.workload_status_message

    entity_name = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "entity-name"
    )
    entity_pass = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "entity-password"
    )

    assert entity_name == "admin"
    assert entity_pass == "password"


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_opensearch_roles_relation_with_charm_libraries_secrets(juju: JujuFixture):
    """Test basic functionality of opensearch relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        OPENSEARCH_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}"
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the index_entity_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "opensearch_entity_created"
    # check if index role is created
    for unit in juju.ext.model.applications[OPENSEARCH_APP_NAME].units:
        assert "created" in unit.workload_status_message

    secret_uri = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_RELATION_NAME, f"{PROV_SECRET_PREFIX}entity"
    )

    secret_content = get_juju_secret(juju, secret_uri)
    entity_name = secret_content["entity-name"]
    entity_pass = secret_content["entity-password"]

    assert entity_name == "admin"
    assert entity_pass == "password"
