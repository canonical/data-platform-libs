#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import subprocess

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_application_relation_data, get_juju_secret, json

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "requirer-app"
APPLICATION_APP_NAME_SPLIT = "requirer-app-split"
KAFKA_APP_NAME = "kafka"
APP_NAMES = [APPLICATION_APP_NAME, APPLICATION_APP_NAME_SPLIT, KAFKA_APP_NAME]
ROLES_RELATION_NAME = "kafka-client-roles"
TOPIC_RELATION_NAME = "kafka-client-topic"
TOPIC_RELATION_NAME_SPLIT_PATTERN = "kafka-split-pattern-client"

PROV_SECRET_PREFIX = "secret-"


@pytest.mark.abort_on_fail
@pytest.mark.log_errors_allowed
@pytest.mark.skip_if_deployed
async def test_deploy_charms(ops_test: OpsTest, application_charm, kafka_charm):
    """Deploy both charms (application and the testing kafka app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm, application_name=APPLICATION_APP_NAME, num_units=1, series="jammy"
        ),
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME_SPLIT,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            kafka_charm, application_name=KAFKA_APP_NAME, num_units=1, series="jammy"
        ),
    )
    await ops_test.model.wait_for_idle(
        apps=[KAFKA_APP_NAME], status="active", wait_for_exact_units=1
    )
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="active", wait_for_exact_units=1
    )
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME_SPLIT], status="active", wait_for_exact_units=1
    )


@pytest.mark.abort_on_fail
async def test_kafka_relation_with_charm_libraries_secrets(ops_test: OpsTest):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{TOPIC_RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in ops_test.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    # Get the requests
    requests = json.loads(
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "requests"
        )
        or "[]"
    )
    request = requests[0]
    secret_uri = request[f"{PROV_SECRET_PREFIX}user"]
    secret_data = await get_juju_secret(ops_test, secret_uri)
    username = secret_data["username"]
    password = secret_data["password"]
    bootstrap_server = request["endpoints"]
    consumer_group_prefix = request["consumer-group-prefix"]
    topic = request["resource"]

    assert username == "admin"
    assert password == "password"
    assert bootstrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "test-prefix"
    assert topic == "test-topic"


async def test_kafka_bootstrap_server_changed(ops_test: OpsTest):
    """Test that the bootstrap server changed event is correctly triggered."""
    app_unit = ops_test.model.applications[APPLICATION_APP_NAME].units[0]
    kafka_unit = ops_test.model.applications[KAFKA_APP_NAME].units[0]
    # set new bootstrap
    parameters = {"bootstrap-server": "host1:port,host2:port,host3:port"}
    action = await kafka_unit.run_action(action_name="sync-bootstrap-server", **parameters)
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["bootstrap-server"] == "host1:port,host2:port,host3:port"

    # check that the new bootstrap-server is in the databag
    requests = json.loads(
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "requests"
        )
        or "[]"
    )
    request = requests[0]
    bootstrap_server = request["endpoints"]
    assert bootstrap_server == "host1:port,host2:port,host3:port"

    # check that the bootstrap_server_changed event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_bootstrap_server_changed"

    # reset unit message
    action = await app_unit.run_action(action_name="reset-unit-status")
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    # check if the message is empty
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""
    # configure the same bootstrap-server
    action = await kafka_unit.run_action(action_name="sync-bootstrap-server", **parameters)
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["bootstrap-server"] == "host1:port,host2:port,host3:port"
    # check that the new bootstrap-server is in the databag
    requests = json.loads(
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "requests"
        )
        or "[]"
    )
    request = requests[0]
    bootstrap_server = request["endpoints"]
    assert bootstrap_server == "host1:port,host2:port,host3:port"
    # check the bootstrap_server_changed event is NOT triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_kafka_mtls(ops_test: OpsTest):
    """Tests mtls-cert is set as a secret from the requirer side and proper event triggered on provider side."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME_SPLIT}:{TOPIC_RELATION_NAME_SPLIT_PATTERN}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    app_unit = ops_test.model.applications[APPLICATION_APP_NAME_SPLIT].units[0]
    action = await app_unit.run_action(action_name="set-mtls-cert")
    _ = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    requests = json.loads(
        await get_application_relation_data(
            ops_test,
            KAFKA_APP_NAME,
            TOPIC_RELATION_NAME,
            "requests",
            related_endpoint=TOPIC_RELATION_NAME_SPLIT_PATTERN,
        )
        or "[]"
    )
    request = requests[0]
    secret_uri = request[f"{PROV_SECRET_PREFIX}mtls"]
    secret_content = await get_juju_secret(ops_test, secret_uri)
    mtls_cert = secret_content["mtls-cert"]

    kafka_unit = ops_test.model.applications[KAFKA_APP_NAME].units[0]
    provider_cert_path = kafka_unit.workload_status_message
    unit_cert = subprocess.check_output(
        f"juju ssh {kafka_unit.name} cat {provider_cert_path}", shell=True, universal_newlines=True
    )

    assert unit_cert.strip() == mtls_cert.strip()


@pytest.mark.abort_on_fail
async def test_kafka_roles_relation_with_charm_libraries_secrets(ops_test: OpsTest):
    """Test basic functionality of kafka-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_entity_created"
    # check if the topic was granted
    for unit in ops_test.model.applications[KAFKA_APP_NAME].units:
        assert "created" in unit.workload_status_message

    requests = json.loads(
        await get_application_relation_data(
            ops_test, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "requests"
        )
        or "[]"
    )
    request = requests[0]

    secret_uri = request[f"{PROV_SECRET_PREFIX}entity"]

    secret_content = await get_juju_secret(ops_test, secret_uri)
    entity_name = secret_content["entity-name"]
    entity_pass = secret_content["entity-password"]

    assert entity_name == "admin"
    assert entity_pass == "password"
