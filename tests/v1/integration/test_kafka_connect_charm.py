#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
from pathlib import Path

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_application_relation_data, get_juju_secret, json

logger = logging.getLogger(__name__)

REQUIRER_APP_NAME = "requirer-app"
PROVIDER_APP_NAME = "kafka-connect"
APP_NAMES = [REQUIRER_APP_NAME, PROVIDER_APP_NAME]
SOURCE_REL = "connect-source"
SINK_REL = "connect-sink"
PROV_SECRET_PREFIX = "secret-"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
@pytest.mark.log_errors_allowed(
    'ERROR juju.worker.meterstatus error running "meter-status-changed": charm missing from disk'
)
async def test_deploy_charms(
    ops_test: OpsTest, application_charm: Path, kafka_connect_charm: Path
):
    """Test deployment of Kafka Connect provider and requirer toy charms."""
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm, application_name=REQUIRER_APP_NAME, num_units=1, series="jammy"
        ),
        ops_test.model.deploy(
            kafka_connect_charm, application_name=PROVIDER_APP_NAME, num_units=1, series="jammy"
        ),
    )

    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        idle_period=30,
        timeout=1800,
        status="active",
    )

    assert ops_test.model.applications[REQUIRER_APP_NAME].status == "active"
    assert ops_test.model.applications[PROVIDER_APP_NAME].status == "active"


@pytest.mark.abort_on_fail
async def test_connect_client_relation_with_charm_libraries(
    ops_test: OpsTest, request: pytest.FixtureRequest
):
    """Test basic functionality of Kafka Connect client relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(PROVIDER_APP_NAME, f"{REQUIRER_APP_NAME}:{SOURCE_REL}")
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit messagge on requirer side
    for unit in ops_test.model.applications[REQUIRER_APP_NAME].units:
        assert unit.workload_status_message == "connect_integration_created"
    # check unit message on provider side
    for unit in ops_test.model.applications[PROVIDER_APP_NAME].units:
        assert "successful" in unit.workload_status_message

    requests = json.loads(
        await get_application_relation_data(ops_test, REQUIRER_APP_NAME, SOURCE_REL, "requests")
        or "[]"
    )
    _request = requests[0]
    secret_uri = _request[f"{PROV_SECRET_PREFIX}user"]

    secret_content = await get_juju_secret(ops_test, secret_uri)
    username = secret_content["username"]
    password = secret_content["password"]

    endpoints = _request["endpoints"]

    request.config.cache.set("initial_password", password)
    request.config.cache.set("initial_endpoints", endpoints)

    assert username == "integrator"
    assert password == "password"
    assert endpoints == "http://worker1:8083,http://worker2:8083"


@pytest.mark.abort_on_fail
async def test_kafka_connect_credentials_change(ops_test: OpsTest, request: pytest.FixtureRequest):
    """Test Kafka Connect credentials change functionality."""
    # Get current password
    password = request.config.cache.get("initial_password", "")
    assert password == "password"

    import pdb

    pdb.set_trace()

    # Change connect password
    action = (
        await ops_test.model.applications[PROVIDER_APP_NAME]
        .units[0]
        .run_action("sync", key="password", value="newpass")
    )
    await action.wait()

    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        idle_period=20,
        timeout=600,
        status="active",
    )

    secret_uri = (
        await get_application_relation_data(
            ops_test, REQUIRER_APP_NAME, SOURCE_REL, f"{PROV_SECRET_PREFIX}user"
        )
        or ""
    )

    requests = json.loads(
        await get_application_relation_data(ops_test, REQUIRER_APP_NAME, SOURCE_REL, "requests")
        or "[]"
    )
    _request = requests[0]
    secret_uri = _request[f"{PROV_SECRET_PREFIX}user"]

    secret_content = await get_juju_secret(ops_test, secret_uri)
    new_password = secret_content["password"]

    assert password != new_password
    assert new_password == "newpass"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_kafka_connect_endpoints_change(ops_test: OpsTest, request: pytest.FixtureRequest):
    """Test Kafka Connect endpoints change functionality."""
    # Get current password
    endpoints = request.config.cache.get("initial_endpoints", "")
    assert endpoints == "http://worker1:8083,http://worker2:8083"

    # Change connect endpoints
    action = (
        await ops_test.model.applications[PROVIDER_APP_NAME]
        .units[0]
        .run_action("sync", key="endpoints", value="http://worker1:8083")
    )
    await action.wait()

    await ops_test.model.wait_for_idle(
        apps=APP_NAMES,
        idle_period=20,
        timeout=600,
        status="active",
    )

    requests = json.loads(
        await get_application_relation_data(ops_test, REQUIRER_APP_NAME, SOURCE_REL, "requests")
        or "[]"
    )
    _request = requests[0]
    new_endpoints = _request["endpoints"]

    assert endpoints != new_endpoints
    assert new_endpoints == "http://worker1:8083"
