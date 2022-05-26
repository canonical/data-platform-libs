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

from tests.integration.helpers import build_connection_string, get_data_from_application

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/integration/database-charm/metadata.yaml").read_text()
)


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, application_charm, database_charm):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms.
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
        ),
        ops_test.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=DATABASE_APP_NAME,
        ),
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", wait_for_units=1)


@pytest.mark.abort_on_fail
async def test_database_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of database relation interface."""
    # Relate the charms and wait for their exchange some connection data.
    await ops_test.model.add_relation(APPLICATION_APP_NAME, DATABASE_APP_NAME)
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the version of the database.
    version = await get_data_from_application(ops_test, APPLICATION_APP_NAME, "version")

    # Get the connection string to connect to the database.
    connection_string = await build_connection_string(ops_test, APPLICATION_APP_NAME)

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
        assert version == data[0]


async def test_user_with_extra_roles(ops_test: OpsTest):
    """Test superuser actions and the request for more permissions."""
    # Define some SQL statements.
    create_database_statement = "CREATE DATABASE another_database;"
    create_user_statement = "CREATE USER another_user WITH ENCRYPTED PASSWORD 'test-password';"

    # Get the connection string to connect to the database.
    connection_string = await build_connection_string(ops_test, APPLICATION_APP_NAME)

    # Connect to the database.
    connection = psycopg2.connect(connection_string)
    connection.autocommit = True
    cursor = connection.cursor()

    # Check that the user can't create a new database or a new user
    # (the user doesn't have this specific roles)
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cursor.execute(create_database_statement)
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cursor.execute(create_user_statement)

    # Request extra user roles calling the action that uses the charm library.
    unit = ops_test.model.units.get(f"{APPLICATION_APP_NAME}/0")
    action = await unit.run_action("request-extra-user-roles", **{"roles": "CREATEDB,CREATEROLE"})
    await action.wait()

    # Wait for the extra roles to be added to the user.
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Test the database and user creation again.
    cursor.execute(create_database_statement)
    cursor.execute(create_user_statement)
