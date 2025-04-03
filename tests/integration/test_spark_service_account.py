#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import json
import logging

import pytest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "app"
SA_PROVIDER_APP_NAME = "sa-provider"
APP_NAMES = [APPLICATION_APP_NAME, SA_PROVIDER_APP_NAME]

RELATION_NAME = "spark-service-account"


@pytest.mark.abort_on_fail
async def test_deploy_charms(
    ops_test: OpsTest,
    application_spark_service_account_charm,
    spark_service_account_charm,
    dp_libs_ubuntu_series,
):
    """Deploy both charms (application and service account provider app) to use in the tests."""
    await asyncio.gather(
        ops_test.model.deploy(
            application_spark_service_account_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
        ops_test.model.deploy(
            spark_service_account_charm,
            application_name=SA_PROVIDER_APP_NAME,
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
    )
    await ops_test.model.wait_for_idle(apps=[SA_PROVIDER_APP_NAME], status="active")
    await ops_test.model.wait_for_idle(apps=[APPLICATION_APP_NAME], status="waiting")

    assert ops_test.model.applications[SA_PROVIDER_APP_NAME].status == "active"
    assert ops_test.model.applications[APPLICATION_APP_NAME].status == "waiting"
    assert (
        ops_test.model.applications[APPLICATION_APP_NAME].status_message
        == "Waiting for spark-service-account relation"
    )


@pytest.mark.abort_on_fail
async def test_spark_service_account_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of spark_service_account relation interface."""
    # Relate the charms and wait for them exchanging some data.
    await ops_test.model.add_relation(
        SA_PROVIDER_APP_NAME, f"{APPLICATION_APP_NAME}:{RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    assert ops_test.model.applications[APPLICATION_APP_NAME].status == "active"
    assert (
        ops_test.model.applications[APPLICATION_APP_NAME].status_message
        == "Service account granted: default:user1"
    )

    app_unit = ops_test.model.applications[APPLICATION_APP_NAME].units[0]
    action = await app_unit.run_action(
        action_name="get-spark-properties",
    )
    result = await action.wait()
    spark_properties = json.loads(result.results.get("spark-properties", "{}"))

    assert spark_properties["default-key"] == "default-val"


@pytest.mark.abort_on_fail
async def test_spark_properties_changed(ops_test: OpsTest):
    """Test the change in spark properties get reflected in requirer charm."""
    provider_unit = ops_test.model.applications[SA_PROVIDER_APP_NAME].units[0]
    action = await provider_unit.run_action(
        action_name="add-spark-property", conf="new-key=new-val"
    )
    result = await action.wait()
    assert result.results.get("success") == "true"

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    app_unit = ops_test.model.applications[APPLICATION_APP_NAME].units[0]
    action = await app_unit.run_action(
        action_name="get-spark-properties",
    )
    result = await action.wait()
    spark_properties = json.loads(result.results.get("spark-properties", "{}"))

    assert spark_properties["default-key"] == "default-val"
    assert spark_properties["new-key"] == "new-val"


@pytest.mark.abort_on_fail
async def test_spark_service_account_relation_broken(ops_test: OpsTest):
    """Test what happens when spark service account relation is broken."""
    await ops_test.model.applications[SA_PROVIDER_APP_NAME].remove_relation(
        f"{SA_PROVIDER_APP_NAME}:spark-service-account", f"{APPLICATION_APP_NAME}:{RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=[SA_PROVIDER_APP_NAME], status="active")
    await ops_test.model.wait_for_idle(apps=[APPLICATION_APP_NAME], status="waiting")

    assert ops_test.model.applications[SA_PROVIDER_APP_NAME].status == "active"
    assert ops_test.model.applications[APPLICATION_APP_NAME].status == "waiting"
    assert (
        ops_test.model.applications[APPLICATION_APP_NAME].status_message
        == "Waiting for spark-service-account relation"
    )

    app_unit = ops_test.model.applications[APPLICATION_APP_NAME].units[0]
    action = await app_unit.run_action(
        action_name="get-spark-properties",
    )
    result = await action.wait()
    spark_properties = json.loads(result.results.get("spark-properties", "{}"))
    assert len(spark_properties) == 0
