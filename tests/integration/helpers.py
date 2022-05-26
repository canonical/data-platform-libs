#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

from pytest_operator.plugin import OpsTest


async def build_connection_string(ops_test: OpsTest, application_name: str) -> str:
    """Get data using an action.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        host: connection data (username, password, database, etc.)

    Returns:
        the connection data that was requested
    """
    # Get the connection data exposed to the application through the relation.
    database = await get_data_from_application(ops_test, application_name, "database")
    username = await get_data_from_application(ops_test, application_name, "username")
    endpoints = await get_data_from_application(ops_test, application_name, "endpoints")
    host = endpoints.split(",")[0].split(":")[0]
    password = await get_data_from_application(ops_test, application_name, "password")

    # Build the complete connection string to connect to the database.
    return f"dbname='{database}' user='{username}' host='{host}' password='{password}' connect_timeout=10"


async def get_data_from_application(ops_test: OpsTest, application_name: str, data: str) -> str:
    """Get data from the application using an action.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        data: data to be retrieved (it has the same name of an action)

    Returns:
        the connection data that was requested
    """
    unit = ops_test.model.units.get(f"{application_name}/0")
    action = await unit.run_action(f"get-{data}")
    result = await action.wait()
    return result.results[data]
