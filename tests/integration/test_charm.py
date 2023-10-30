#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import json
import logging
from pathlib import Path
from time import sleep

import psycopg2
import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .helpers import (
    build_connection_string,
    get_application_relation_data,
    get_juju_secret,
    get_leader_id,
    get_non_leader_id,
    list_juju_secrets,
)

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
ANOTHER_DATABASE_APP_NAME = "another-database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME, ANOTHER_DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/integration/database-charm/metadata.yaml").read_text()
)
FIRST_DATABASE_RELATION_NAME = "first-database"
SECOND_DATABASE_RELATION_NAME = "second-database"
MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "multiple-database-clusters"
ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "aliased-multiple-database-clusters"

SECRET_REF_PREFIX = "secret-"


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, application_charm, database_charm):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (1 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm, application_name=APPLICATION_APP_NAME, num_units=2, series="jammy"
        ),
        ops_test.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=DATABASE_APP_NAME,
            num_units=2,
            series="jammy",
        ),
        ops_test.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=ANOTHER_DATABASE_APP_NAME,
            series="jammy",
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME, DATABASE_APP_NAME], status="active", wait_for_exact_units=2
    )
    await ops_test.model.wait_for_idle(
        apps=[ANOTHER_DATABASE_APP_NAME], status="active", wait_for_exact_units=1
    )


@pytest.mark.abort_on_fail
async def test_database_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of database relation interface."""
    # Relate the charms and wait for them exchanging some connection data.

    pytest.first_database_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection string to connect to the database.
    connection_string = await build_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )

    # Connect to the database.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that it's possible to write and read data from the database that
        # was created for the application.
        connection.autocommit = True
        cursor.execute("DROP TABLE IF EXISTS test;")
        cursor.execute("CREATE TABLE test(data TEXT);")
        cursor.execute("INSERT INTO test(data) VALUES('some data');")
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

        # Check the version that the application received is the same on the database server.
        cursor.execute("SELECT version();")
        data = cursor.fetchone()

        # Get the version of the database and compare with the information that
        # was retrieved directly from the database.
        version = await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "version"
        )
        assert version == data[0]


async def test_user_with_extra_roles(ops_test: OpsTest):
    """Test superuser actions and the request for more permissions."""
    # Get the connection string to connect to the database.
    connection_string = await build_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )

    # Connect to the database.
    connection = psycopg2.connect(connection_string)
    connection.autocommit = True
    cursor = connection.cursor()

    # Test the user can create a database and another user.
    cursor.execute("CREATE DATABASE another_database;")
    cursor.execute("CREATE USER another_user WITH ENCRYPTED PASSWORD 'test-password';")

    cursor.close()
    connection.close()


@pytest.mark.abort_on_fail
async def test_postgresql_plugin(ops_test: OpsTest):
    """Test that the application charm can check whether a plugin is enabled."""
    # Check that the plugin is disabled.
    unit_name = f"{APPLICATION_APP_NAME}/0"
    action = await ops_test.model.units.get(unit_name).run_action(
        "get-plugin-status", **{"plugin": "citext"}
    )
    await action.wait()
    assert action.results.get("plugin-status") == "disabled"

    # Connect to the database and enable the plugin (PostgreSQL extension).
    connection_string = await build_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        connection.autocommit = True
        cursor.execute("CREATE EXTENSION citext;")
    connection.close()

    # Check that the plugin is enabled.
    action = await ops_test.model.units.get(unit_name).run_action(
        "get-plugin-status", **{"plugin": "citext"}
    )
    await action.wait()
    assert action.results.get("plugin-status") == "enabled"


async def test_two_applications_dont_share_the_same_relation_data(
    ops_test: OpsTest, application_charm
):
    """Test that two different application connect to the database with different credentials."""
    # Set some variables to use in this test.
    another_application_app_name = "another-application"
    all_app_names = [another_application_app_name]
    all_app_names.extend(APP_NAMES)

    # Deploy another application.
    await ops_test.model.deploy(
        application_charm, application_name=another_application_app_name, series="jammy"
    )
    await ops_test.model.wait_for_idle(apps=all_app_names, status="active")

    # Relate the new application with the database
    # and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        f"{another_application_app_name}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=all_app_names, status="active")

    # Assert the two application have different relation (connection) data.
    application_connection_string = await build_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    another_application_connection_string = await build_connection_string(
        ops_test, another_application_app_name, FIRST_DATABASE_RELATION_NAME
    )
    assert application_connection_string != another_application_connection_string


@pytest.mark.usefixtures("only_without_juju_secrets")
async def test_databag_usage_correct(ops_test: OpsTest, application_charm):
    for field in ["username", "password"]:
        assert await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, field
        )


@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_secrets_usage_correct_secrets(ops_test: OpsTest, application_charm):
    for field in ["username", "password", "uris"]:
        assert (
            await get_application_relation_data(
                ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, field
            )
            is None
        )
    assert await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
    )


async def test_an_application_can_connect_to_multiple_database_clusters(
    ops_test: OpsTest, database_charm
):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    first_cluster_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}", DATABASE_APP_NAME
    )
    # This call enables the unit to be available in the relation changed event.
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    second_cluster_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = await build_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_id=first_cluster_relation.id,
    )
    another_application_connection_string = await build_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_id=second_cluster_relation.id,
    )
    assert application_connection_string != another_application_connection_string


async def test_an_application_can_connect_to_multiple_aliased_database_clusters(
    ops_test: OpsTest, database_charm
):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        DATABASE_APP_NAME,
    )
    # This call enables the unit to be available in the relation changed event.
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = await build_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_alias="cluster1",
    )
    another_application_connection_string = await build_connection_string(
        ops_test,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_alias="cluster2",
    )
    assert application_connection_string != another_application_connection_string


async def test_an_application_can_request_multiple_databases(ops_test: OpsTest, application_charm):
    """Test that an application can request additional databases using the same interface."""
    # Relate the charms using another relation and wait for them exchanging some connection data.
    sleep(5)
    pytest.second_database_relation = await ops_test.model.add_relation(
        f"{APPLICATION_APP_NAME}:{SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection strings to connect to both databases.
    first_database_connection_string = await build_connection_string(
        ops_test, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME
    )
    second_database_connection_string = await build_connection_string(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME
    )

    # Assert the two application have different relation (connection) data.
    assert first_database_connection_string != second_database_connection_string


@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_provider_with_additional_secrets(ops_test: OpsTest, database_charm):
    # Let's make sure that there was enough time for the relation initialization to communicate secrets
    secret_fields = await get_application_relation_data(
        ops_test,
        DATABASE_APP_NAME,
        DATABASE_APP_NAME,
        "requested-secrets",
        related_endpoint=SECOND_DATABASE_RELATION_NAME,
    )
    assert {"topsecret", "donttellanyone"} <= set(json.loads(secret_fields))

    # Set secret
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-secret", **{"relation_id": pytest.second_database_relation.id, "field": "topsecret"}
    )
    await action.wait()

    # Get secret original value
    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, f"{SECRET_REF_PREFIX}extra"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    topsecret1 = secret_content["topsecret"]

    # Re-set secret
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-secret", **{"relation_id": pytest.second_database_relation.id, "field": "topsecret"}
    )
    await action.wait()

    # Get secret after change
    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, f"{SECRET_REF_PREFIX}extra"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    topsecret2 = secret_content["topsecret"]

    assert topsecret1 != topsecret2


@pytest.mark.parametrize("field,value", [("new_field", "blah"), ("tls", "True")])
@pytest.mark.usefixtures("only_without_juju_secrets")
async def test_provider_get_set_delete_fields(field, value, ops_test: OpsTest):
    # Add normal field
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, field
        )
        == value
    )

    # Check all application units can read remote relation data
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        action = await unit.run_action(
            "get-relation-field",
            **{
                "relation_id": pytest.second_database_relation.id,
                "field": field,
            },
        )
        await action.wait()
        assert action.results.get("value") == value

    # Check if database can retrieve self-side relation data
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    await action.wait()
    assert action.results.get("value") == value

    # Delete normal field
    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": field},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, field
        )
        is None
    )


@pytest.mark.parametrize(
    "field,value,relation_field",
    [
        ("new_field", "blah", "new_field"),
        pytest.param(
            "tls",
            "True",
            "secret-tls",
            marks=pytest.mark.xfail(
                reason="https://github.com/canonical/data-platform-libs/issues/108"
            ),
        ),
    ],
)
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_provider_get_set_delete_fields_secrets(
    field, value, relation_field, ops_test: OpsTest
):
    # Add field
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    await action.wait()

    assert await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, relation_field
    )

    # Check all application units can read remote relation data
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        action = await unit.run_action(
            "get-relation-field",
            **{
                "relation_id": pytest.second_database_relation.id,
                "field": field,
            },
        )
        await action.wait()
        assert action.results.get("value") == value

    # Check if database can retrieve self-side relation data
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    await action.wait()
    assert action.results.get("value") == value

    # Delete normal field
    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": field},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, relation_field
        )
        is None
    )

    # Check that the field is deleted
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
        },
    )
    await action.wait()
    assert not action.results.get("value")


@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_provider_deleted_secret_is_removed(ops_test: OpsTest):
    """The 'tls' field, that was removed in the previous test has it's secret removed."""
    # Get TLS secret pointer
    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, f"{SECRET_REF_PREFIX}tls"
    )

    # The 7 lines below can be removed once the test above is fully passing
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls"},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "tls"
        )
        is None
    )

    secrets = await list_juju_secrets(ops_test)
    secret_xid = secret_uri.split("/")[-1]
    assert secret_xid not in secrets


async def test_requires_get_set_delete_fields(ops_test: OpsTest):
    # Add normal field
    leader_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_name = f"{APPLICATION_APP_NAME}/{leader_id}"

    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field",
            "value": "blah",
        },
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test,
            DATABASE_APP_NAME,
            SECOND_DATABASE_RELATION_NAME,
            "new_field",
            related_endpoint="second-database",
        )
        == "blah"
    )

    # Check all application units can read remote relation data
    for unit in ops_test.model.applications[DATABASE_APP_NAME].units:
        action = await unit.run_action(
            "get-relation-field",
            **{
                "relation_id": pytest.second_database_relation.id,
                "field": "new_field",
            },
        )
        await action.wait()
        assert action.results.get("value") == "blah"

    # Check if database can retrieve self-side relation data
    action = await ops_test.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field",
            "value": "blah",
        },
    )
    await action.wait()
    assert action.results.get("value") == "blah"

    # Delete normal field
    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field"},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test,
            DATABASE_APP_NAME,
            SECOND_DATABASE_RELATION_NAME,
            "new_field",
            related_endpoint="second-database",
        )
        is None
    )


async def test_provider_set_delete_fields_leader_only(ops_test: OpsTest):
    leader_id = await get_leader_id(ops_test, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field",
            "value": "blah",
        },
    )
    await action.wait()

    unit_id = await get_non_leader_id(ops_test, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{unit_id}"
    action = await ops_test.model.units.get(unit_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field2",
            "value": "blah2",
        },
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "new_field2"
        )
        is None
    )

    action = await ops_test.model.units.get(unit_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field"},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, "new_field"
        )
        == "blah"
    )


async def test_requires_set_delete_fields(ops_test: OpsTest):
    # Add field
    leader_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_name = f"{APPLICATION_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field_req",
            "value": "blah-req",
        },
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field_req",
            related_endpoint=SECOND_DATABASE_RELATION_NAME,
        )
        == "blah-req"
    )

    # Delete field
    action = await ops_test.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field_req"},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field_req",
            related_endpoint=SECOND_DATABASE_RELATION_NAME,
        )
        is None
    )


async def test_requires_set_delete_fields_leader_only(ops_test: OpsTest):
    leader_id = await get_leader_id(ops_test, APPLICATION_APP_NAME)
    leader_name = f"{APPLICATION_APP_NAME}/{leader_id}"
    action = await ops_test.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field-req",
            "value": "blah-req",
        },
    )
    await action.wait()

    unit_id = await get_non_leader_id(ops_test, APPLICATION_APP_NAME)
    unit_name = f"{APPLICATION_APP_NAME}/{unit_id}"
    action = await ops_test.model.units.get(unit_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field2-req",
            "value": "blah2-req",
        },
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field2-req",
            related_endpoint=SECOND_DATABASE_RELATION_NAME,
        )
        is None
    )

    action = await ops_test.model.units.get(unit_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field-req"},
    )
    await action.wait()

    assert (
        await get_application_relation_data(
            ops_test,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field-req",
            related_endpoint=SECOND_DATABASE_RELATION_NAME,
        )
        == "blah-req"
    )


async def test_scaling_requires_can_access_shared_secrest(ops_test):
    """When scaling up the application, new units should have access to relation secrets."""
    await ops_test.model.applications[APPLICATION_APP_NAME].scale(3)

    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="active", timeout=(15 * 60), wait_for_exact_units=3
    )

    old_unit_name = f"{APPLICATION_APP_NAME}/1"
    new_unit_name = f"{APPLICATION_APP_NAME}/2"

    action = await ops_test.model.units.get(old_unit_name).run_action(
        "get-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "password",
        },
    )
    await action.wait()
    orig_password = action.results.get("value")

    action = await ops_test.model.units.get(new_unit_name).run_action(
        "get-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "password",
        },
    )
    await action.wait()
    new_password = action.results.get("value")
    assert new_password == orig_password
