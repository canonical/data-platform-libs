#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import Optional

import yaml
from pytest_operator.plugin import OpsTest


async def build_connection_string(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    *,
    relation_id: str = None,
    relation_alias: str = None,
) -> str:
    """Build a PostgreSQL connection string.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        relation_id: id of the relation to get connection data from
        relation_alias: alias of the relation (like a connection name)
            to get connection data from

    Returns:
        a PostgreSQL connection string
    """
    # Get the connection data exposed to the application through the relation.
    database = f'{application_name.replace("-", "_")}_{relation_name.replace("-", "_")}'
    username = await get_application_relation_data(
        ops_test, application_name, relation_name, "username", relation_id, relation_alias
    )
    password = await get_application_relation_data(
        ops_test, application_name, relation_name, "password", relation_id, relation_alias
    )
    endpoints = await get_application_relation_data(
        ops_test, application_name, relation_name, "endpoints", relation_id, relation_alias
    )
    host = endpoints.split(",")[0].split(":")[0]

    # Build the complete connection string to connect to the database.
    return f"dbname='{database}' user='{username}' host='{host}' password='{password}' connect_timeout=10"


async def get_alias_from_relation_data(
    ops_test: OpsTest, unit_name: str, related_unit_name: str
) -> Optional[str]:
    """Get the alias that the unit assigned to the related unit application/cluster.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit
        related_unit_name: name of the related unit

    Returns:
        the alias for the application/cluster of
            the related unit

    Raises:
        ValueError if it's not possible to get unit data
            or if there is no alias on that.
    """
    raw_data = (await ops_test.juju("show-unit", related_unit_name))[1]
    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {related_unit_name}")
    data = yaml.safe_load(raw_data)

    # Retrieve the relation data from the unit.
    relation_data = {}
    for relation in data[related_unit_name]["relation-info"]:
        for name, unit in relation["related-units"].items():
            if name == unit_name:
                relation_data = unit["data"]
                break

    # Check whether the unit has set an alias for the related unit application/cluster.
    if "alias" not in relation_data:
        raise ValueError(f"no alias could be grabbed for {related_unit_name} application/cluster")

    return relation_data["alias"]


async def get_application_relation_data(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    key: str,
    relation_id: str = None,
    relation_alias: str = None,
) -> Optional[str]:
    """Get relation data for an application.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        key: key of data to be retrieved
        relation_id: id of the relation to get connection data from
        relation_alias: alias of the relation (like a connection name)
            to get connection data from

    Returns:
        the data that was requested or None
            if no data in the relation

    Raises:
        ValueError if it's not possible to get application data
            or if there is no data for the particular relation endpoint
            and/or alias.
    """
    unit_name = f"{application_name}/0"
    raw_data = (await ops_test.juju("show-unit", unit_name))[1]
    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {unit_name}")
    data = yaml.safe_load(raw_data)
    # Filter the data based on the relation name.
    relation_data = [v for v in data[unit_name]["relation-info"] if v["endpoint"] == relation_name]
    if relation_id:
        # Filter the data based on the relation id.
        relation_data = [v for v in relation_data if v["relation-id"] == relation_id]
    if relation_alias:
        # Filter the data based on the cluster/relation alias.
        relation_data = [
            v
            for v in relation_data
            if await get_alias_from_relation_data(
                ops_test, unit_name, next(iter(v["related-units"]))
            )
            == relation_alias
        ]
    if len(relation_data) == 0:
        raise ValueError(
            f"no relation data could be grabbed on relation with endpoint {relation_name} and alias {relation_alias}"
        )
    return relation_data[0]["application-data"].get(key)
