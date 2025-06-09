#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import subprocess

import pytest
from pytest_operator.plugin import OpsTest

from .helpers import get_application_relation_data, get_juju_secret

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "requirer-app"
APPLICATION_APP_NAME_SPLIT = "requirer-app-split"
KAFKA_APP_NAME = "kafka"
APP_NAMES = [APPLICATION_APP_NAME, APPLICATION_APP_NAME_SPLIT, KAFKA_APP_NAME]
RELATION_NAME = "kafka-client"
RELATION_NAME_SPLIT_PATTERN = "kafka-split-pattern-client"

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
@pytest.mark.usefixtures("only_without_juju_secrets")
async def test_kafka_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{RELATION_NAME}")
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit messagge to check if the topic_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in ops_test.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "username"
    )
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "password"
    )

    bootstrap_server = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "endpoints"
    )

    consumer_group_prefix = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "consumer-group-prefix"
    )

    topic = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "topic"
    )

    assert username == "admin"
    assert password == "password"
    assert bootstrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "test-prefix"
    assert topic == "test-topic"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_without_juju_secrets")
async def test_kafka_relation_with_charm_libraries_split_pattern(ops_test: OpsTest):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME_SPLIT}:{RELATION_NAME_SPLIT_PATTERN}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit messagge to check if the topic_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME_SPLIT].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in ops_test.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME_SPLIT, RELATION_NAME_SPLIT_PATTERN, "username"
    )
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME_SPLIT, RELATION_NAME_SPLIT_PATTERN, "password"
    )

    bootstrap_server = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME_SPLIT, RELATION_NAME_SPLIT_PATTERN, "endpoints"
    )

    consumer_group_prefix = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME_SPLIT, RELATION_NAME_SPLIT_PATTERN, "consumer-group-prefix"
    )

    topic = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME_SPLIT, RELATION_NAME_SPLIT_PATTERN, "topic"
    )

    assert username == "admin"
    assert password == "password"
    assert bootstrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "test-prefix"
    assert topic == "test-topic-split-pattern"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("only_with_juju_secrets")
async def test_kafka_relation_with_charm_libraries_secrets(ops_test: OpsTest):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{RELATION_NAME}")
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit messagge to check if the topic_created_event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in ops_test.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    secret_uri = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    username = secret_content["username"]
    password = secret_content["password"]

    bootstrap_server = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "endpoints"
    )

    consumer_group_prefix = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "consumer-group-prefix"
    )

    topic = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "topic"
    )

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
    bootstrap_server = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "endpoints"
    )
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
    bootstrap_server = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "endpoints"
    )
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
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME_SPLIT}:{RELATION_NAME_SPLIT_PATTERN}"
    )
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    app_unit = ops_test.model.applications[APPLICATION_APP_NAME_SPLIT].units[0]
    action = await app_unit.run_action(action_name="set-mtls-cert")
    _ = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    secret_uri = await get_application_relation_data(
        ops_test,
        KAFKA_APP_NAME,
        RELATION_NAME,
        f"{PROV_SECRET_PREFIX}mtls",
        related_endpoint=RELATION_NAME_SPLIT_PATTERN,
    )

    secret_content = await get_juju_secret(ops_test, secret_uri)
    mtls_cert = secret_content["mtls-cert"]

    kafka_unit = ops_test.model.applications[KAFKA_APP_NAME].units[0]
    provider_cert_path = kafka_unit.workload_status_message
    unit_cert = subprocess.check_output(
        f"juju ssh {kafka_unit.name} cat {provider_cert_path}", shell=True, universal_newlines=True
    )

    assert unit_cert.strip() == mtls_cert.strip()
