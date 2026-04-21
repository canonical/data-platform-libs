#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import subprocess

import pytest
from jubilant_adapters import JujuFixture, gather

from .helpers import get_application_relation_data, get_juju_secret

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "requirer-app"
APPLICATION_APP_NAME_SPLIT = "requirer-app-split"
KAFKA_APP_NAME = "kafka"
APP_NAMES = [APPLICATION_APP_NAME, APPLICATION_APP_NAME_SPLIT, KAFKA_APP_NAME]
ROLES_RELATION_NAME = "kafka-client-roles"
TOPIC_RELATION_NAME = "kafka-client-topic"
TOPIC_RELATION_NAME_SPLIT_PATTERN = "kafka-split-pattern-client"

PROV_SECRET_PREFIX = "secret-"


@pytest.mark.log_errors_allowed
def test_deploy_charms(juju: JujuFixture, application_charm, kafka_charm):
    """Deploy both charms (application and the testing kafka app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    gather(
        juju.ext.model.deploy(
            application_charm, application_name=APPLICATION_APP_NAME, num_units=1, series="jammy"
        ),
        juju.ext.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME_SPLIT,
            num_units=1,
            series="jammy",
        ),
        juju.ext.model.deploy(
            kafka_charm, application_name=KAFKA_APP_NAME, num_units=1, series="jammy"
        ),
    )
    juju.ext.model.wait_for_idle(apps=[KAFKA_APP_NAME], status="active", wait_for_exact_units=1)
    juju.ext.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="active", wait_for_exact_units=1
    )
    juju.ext.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME_SPLIT], status="active", wait_for_exact_units=1
    )


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_kafka_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{TOPIC_RELATION_NAME}")
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in juju.ext.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    username = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "username"
    )
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "password"
    )

    bootstrap_server = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "endpoints"
    )

    consumer_group_prefix = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "consumer-group-prefix"
    )

    topic = get_application_relation_data(juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "topic")

    assert username == "admin"
    assert password == "password"
    assert bootstrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "test-prefix"
    assert topic == "test-topic"


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_kafka_relation_with_charm_libraries_split_pattern(juju: JujuFixture):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME_SPLIT}:{TOPIC_RELATION_NAME_SPLIT_PATTERN}"
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME_SPLIT].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in juju.ext.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    username = get_application_relation_data(
        juju, APPLICATION_APP_NAME_SPLIT, TOPIC_RELATION_NAME_SPLIT_PATTERN, "username"
    )
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME_SPLIT, TOPIC_RELATION_NAME_SPLIT_PATTERN, "password"
    )

    bootstrap_server = get_application_relation_data(
        juju, APPLICATION_APP_NAME_SPLIT, TOPIC_RELATION_NAME_SPLIT_PATTERN, "endpoints"
    )

    consumer_group_prefix = get_application_relation_data(
        juju,
        APPLICATION_APP_NAME_SPLIT,
        TOPIC_RELATION_NAME_SPLIT_PATTERN,
        "consumer-group-prefix",
    )

    topic = get_application_relation_data(
        juju, APPLICATION_APP_NAME_SPLIT, TOPIC_RELATION_NAME_SPLIT_PATTERN, "topic"
    )

    assert username == "admin"
    assert password == "password"
    assert bootstrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "test-prefix"
    assert topic == "test-topic-split-pattern"


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_kafka_relation_with_charm_libraries_secrets(juju: JujuFixture):
    """Test basic functionality of kafka relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{TOPIC_RELATION_NAME}")
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_topic_created"
    # check if the topic was granted
    for unit in juju.ext.model.applications[KAFKA_APP_NAME].units:
        assert "granted" in unit.workload_status_message

    secret_uri = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, f"{PROV_SECRET_PREFIX}user"
    )

    secret_content = get_juju_secret(juju, secret_uri)
    username = secret_content["username"]
    password = secret_content["password"]

    bootstrap_server = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "endpoints"
    )

    consumer_group_prefix = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "consumer-group-prefix"
    )

    topic = get_application_relation_data(juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "topic")

    assert username == "admin"
    assert password == "password"
    assert bootstrap_server == "host1:port,host2:port"
    assert consumer_group_prefix == "test-prefix"
    assert topic == "test-topic"


def test_kafka_bootstrap_server_changed(juju: JujuFixture):
    """Test that the bootstrap server changed event is correctly triggered."""
    app_unit = juju.ext.model.applications[APPLICATION_APP_NAME].units[0]
    kafka_unit = juju.ext.model.applications[KAFKA_APP_NAME].units[0]
    # set new bootstrap
    parameters = {"bootstrap-server": "host1:port,host2:port,host3:port"}
    action = kafka_unit.run_action(action_name="sync-bootstrap-server", **parameters)
    result = action.wait()
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["bootstrap-server"] == "host1:port,host2:port,host3:port"
    # check that the new bootstrap-server is in the databag
    bootstrap_server = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "endpoints"
    )
    assert bootstrap_server == "host1:port,host2:port,host3:port"

    # check that the bootstrap_server_changed event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_bootstrap_server_changed"
    # reset unit message
    action = app_unit.run_action(action_name="reset-unit-status")
    result = action.wait()
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")
    # check if the message is empty
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""
    # configure the same bootstrap-server
    action = kafka_unit.run_action(action_name="sync-bootstrap-server", **parameters)
    result = action.wait()
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")
    assert result.results["bootstrap-server"] == "host1:port,host2:port,host3:port"
    bootstrap_server = get_application_relation_data(
        juju, APPLICATION_APP_NAME, TOPIC_RELATION_NAME, "endpoints"
    )
    assert bootstrap_server == "host1:port,host2:port,host3:port"
    # check the bootstrap_server_changed event is NOT triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == ""


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_kafka_mtls(juju: JujuFixture):
    """Tests mtls-cert is set as a secret from the requirer side and proper event triggered on provider side."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        KAFKA_APP_NAME, f"{APPLICATION_APP_NAME_SPLIT}:{TOPIC_RELATION_NAME_SPLIT_PATTERN}"
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    app_unit = juju.ext.model.applications[APPLICATION_APP_NAME_SPLIT].units[0]
    action = app_unit.run_action(action_name="set-mtls-cert")
    _ = action.wait()
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    secret_uri = get_application_relation_data(
        juju,
        KAFKA_APP_NAME,
        TOPIC_RELATION_NAME,
        f"{PROV_SECRET_PREFIX}mtls",
        related_endpoint=TOPIC_RELATION_NAME_SPLIT_PATTERN,
    )

    secret_content = get_juju_secret(juju, secret_uri)
    mtls_cert = secret_content["mtls-cert"]

    kafka_unit = juju.ext.model.applications[KAFKA_APP_NAME].units[0]
    provider_cert_path = kafka_unit.workload_status_message
    unit_cert = subprocess.check_output(
        f"juju ssh {kafka_unit.name} cat {provider_cert_path}", shell=True, universal_newlines=True
    )

    assert unit_cert.strip() == mtls_cert.strip()


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_kafka_roles_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of kafka-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}")
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_entity_created"
    # check if the topic role was granted
    for unit in juju.ext.model.applications[KAFKA_APP_NAME].units:
        assert "created" in unit.workload_status_message

    entity_name = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "entity-name"
    )
    entity_pass = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_RELATION_NAME, "entity-password"
    )

    assert entity_name == "admin"
    assert entity_pass == "password"


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_kafka_roles_relation_with_charm_libraries_secrets(juju: JujuFixture):
    """Test basic functionality of kafka-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(KAFKA_APP_NAME, f"{APPLICATION_APP_NAME}:{ROLES_RELATION_NAME}")
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # check unit message to check if the topic_created_event is triggered
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        assert unit.workload_status_message == "kafka_entity_created"
    # check if the topic was granted
    for unit in juju.ext.model.applications[KAFKA_APP_NAME].units:
        assert "created" in unit.workload_status_message

    secret_uri = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_RELATION_NAME, f"{PROV_SECRET_PREFIX}entity"
    )

    secret_content = get_juju_secret(juju, secret_uri)
    entity_name = secret_content["entity-name"]
    entity_pass = secret_content["entity-password"]

    assert entity_name == "admin"
    assert entity_pass == "password"
