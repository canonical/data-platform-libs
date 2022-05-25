#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

from pytest_operator.plugin import OpsTest


async def get_connection_data(ops_test: OpsTest, application_name: str, data: str) -> str:
    """Get data using an action."""
    unit = ops_test.model.units.get(f"{application_name}/0")
    action = await unit.run_action(f"get-{data}")
    result = await action.wait()
    return result.results[data]
