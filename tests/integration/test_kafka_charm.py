#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging

import pytest
from pytest_operator.plugin import OpsTest

from tests.integration.helpers import get_application_relation_data

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "app"
KAFKA_APP_NAME = "kafka"
APP_NAMES = [APPLICATION_APP_NAME, KAFKA_APP_NAME]
RELATION_NAME = "kafka_client"


@pytest.mark.kafka_tests
@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, application_charm, kafka_charm):
    """Deploy both charms (application and s3 provider app) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=1,
        ),
        ops_test.model.deploy(
            kafka_charm,
            application_name=KAFKA_APP_NAME,
            num_units=1,
        ),
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA_APP_NAME], status="active", wait_for_units=1)
    await ops_test.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="active", wait_for_units=1
    )


@pytest.mark.kafka_tests
@pytest.mark.abort_on_fail
async def test_kafka_relation_with_charm_libraries(ops_test: OpsTest):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(KAFKA_APP_NAME, APPLICATION_APP_NAME)
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

    boostrap_server = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "endpoints"
    )
    consumer_group_prefix = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "consumer-group-prefix"
    )

    assert username == "admin"
    assert password == "password"
    assert boostrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "group1,group2"


@pytest.mark.kafka_tests
@pytest.mark.abort_on_fail
async def test_kafka_credential_changed(ops_test: OpsTest):

    app_unit = ops_test.model.applications[APPLICATION_APP_NAME].units[0]
    kafka_unit = ops_test.model.applications[KAFKA_APP_NAME].units[0]
    # set new password
    parameters = {"password": "new-password"}
    action = await kafka_unit.run_action(action_name="sync-password", **parameters)
    result = await action.wait()
    assert result.results["password"] == "new-password"
    # check that the new password is in the databag
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "password"
    )
    assert password == "new-password"
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check that the credential_changed event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_credentials_changed"
    # reset unit message
    action = await app_unit.run_action(action_name="reset-unit-status")
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    # check if the message is empty
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""
    # configure the same password
    action = await kafka_unit.run_action(action_name="sync-password", **parameters)
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["password"] == "new-password"
    password = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "password"
    )
    assert password == "new-password"
    # check the credential_changed event is NOT triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""

    # set new username
    parameters = {"username": "new-username"}
    action = await kafka_unit.run_action(action_name="sync-username", **parameters)
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["username"] == "new-username"

    # check that the new password is in the databag
    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "username"
    )
    assert username == "new-username"
    # check that the credential_changed event is triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_credentials_changed"

    # reset unit message
    action = await app_unit.run_action(action_name="reset-unit-status")
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    # check if the message is empty
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""

    action = await kafka_unit.run_action(action_name="sync-username", **parameters)
    result = await action.wait()
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["username"] == "new-username"

    # check that the new password is in the databag
    username = await get_application_relation_data(
        ops_test, APPLICATION_APP_NAME, RELATION_NAME, "username"
    )
    assert username == "new-username"
    # check the credential_changed event is NOT triggered
    for unit in ops_test.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""


async def test_kafka_bootstrap_server_changed(ops_test: OpsTest):
    pass


# async def test_two_applications_doesnt_share_the_same_relation_data(
#     ops_test: OpsTest, application_s3_charm
# ):
#     """Test that two applications connect to the s3 provider with different credentials."""
#     # Set some variables to use in this test.
#     another_application_app_name = "another-app"
#     all_app_names = [another_application_app_name]
#     all_app_names.extend(APP_NAMES)

#     # Deploy another application.
#     await ops_test.model.deploy(
#         application_s3_charm,
#         application_name=another_application_app_name,
#     )
#     await ops_test.model.wait_for_idle(apps=[S3_APP_NAME, APPLICATION_APP_NAME], status="active")
#     await ops_test.model.wait_for_idle(apps=[another_application_app_name], status="waiting")
#     # Relate the new application with the s3 provider
#     # and wait for them exchanging some connection data.
#     await ops_test.model.add_relation(
#         f"{another_application_app_name}:{FIRST_S3_RELATION_NAME}", S3_APP_NAME
#     )
#     await ops_test.model.wait_for_idle(apps=all_app_names, status="active")

#     # Assert the two applications have different relation (connection) data.
#     application_connection_info = await get_connection_info(
#         ops_test, APPLICATION_APP_NAME, FIRST_S3_RELATION_NAME
#     )
#     another_application_connection_info = await get_connection_info(
#         ops_test, another_application_app_name, FIRST_S3_RELATION_NAME
#     )

#     assert application_connection_info != another_application_connection_info


# async def test_an_application_can_request_multiple_s3_providers(ops_test: OpsTest):
#     """Test that an application can request additional s3 credentials using the same interface."""
#     # Relate the charms using another relation and wait for them exchanging some connection data.
#     await ops_test.model.add_relation(
#         f"{APPLICATION_APP_NAME}:{SECOND_S3_RELATION_NAME}", S3_APP_NAME
#     )
#     await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")

#     # Get the connection infos from the two different relations.
#     first_s3_connection_info = await get_connection_info(
#         ops_test, APPLICATION_APP_NAME, FIRST_S3_RELATION_NAME
#     )
#     second_s3_connection_info = await get_connection_info(
#         ops_test, APPLICATION_APP_NAME, SECOND_S3_RELATION_NAME
#     )

#     # Assert the two applications have different relation (connection) data.
#     assert first_s3_connection_info != second_s3_connection_info
