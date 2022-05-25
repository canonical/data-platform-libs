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

from tests.integration.helpers import get_connection_data

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/integration/database-charm/metadata.yaml").read_text()
)


@pytest.mark.abort_on_fail
async def test_database_relation_with_charm_libraries(
    ops_test: OpsTest, application_charm, database_charm
):
    """Test basic functionality of database relation interface."""
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

    # Relate the charms and wait for their exchange some connection data.
    await ops_test.model.add_relation(APPLICATION_APP_NAME, DATABASE_APP_NAME)
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the application IP from the database instance.
    status = await ops_test.model.get_status()
    host = status["applications"][DATABASE_APP_NAME].units[f"{DATABASE_APP_NAME}/0"]["address"]

    # Get the connection data exposed to the application through the relation.
    database = await get_connection_data(ops_test, APPLICATION_APP_NAME, "database")
    username = await get_connection_data(ops_test, APPLICATION_APP_NAME, "username")
    password = await get_connection_data(ops_test, APPLICATION_APP_NAME, "password")
    version = await get_connection_data(ops_test, APPLICATION_APP_NAME, "version")

    # Build the complete connection string to connect to the database.
    connection_string = f"dbname='{database}' user='{username}' host='{host}' password='{password}' connect_timeout=10"

    # Test the connection to the database.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that it's possible to write and read data from the database that
        # was created for the application.
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
