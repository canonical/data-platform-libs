#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
from pathlib import Path

import psycopg2
import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .helpers import build_connection_string, get_application_relation_data

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


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, application_charm, database_charm):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
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
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", wait_for_units=1)


@pytest.mark.abort_on_fail
async def test_database_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of database relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
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


async def test_two_applications_doesnt_share_the_same_relation_data(
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
    await ops_test.model.add_relation(
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
