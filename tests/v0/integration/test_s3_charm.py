#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

from jubilant_adapters import JujuFixture, gather

from .helpers import get_connection_info

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "app"
S3_APP_NAME = "s3-provider-app"
APP_NAMES = [APPLICATION_APP_NAME, S3_APP_NAME]
FIRST_S3_RELATION_NAME = "first-s3-credentials"
SECOND_S3_RELATION_NAME = "second-s3-credentials"


def test_deploy_charms(juju: JujuFixture, application_s3_charm, s3_charm, dp_libs_ubuntu_series):
    """Deploy both charms (application and s3 provider app) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    gather(
        juju.ext.model.deploy(
            application_s3_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=2,
            series=dp_libs_ubuntu_series,
        ),
        juju.ext.model.deploy(s3_charm, application_name=S3_APP_NAME, num_units=2, series="jammy"),
    )
    juju.ext.model.wait_for_idle(apps=[S3_APP_NAME], status="active", wait_for_exact_units=2)
    juju.ext.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="waiting", wait_for_exact_units=2
    )


def test_s3_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of s3-credentials relation interface."""
    # Relate the charms and wait for them exchanging some connection data.
    juju.ext.model.add_relation(S3_APP_NAME, f"{APPLICATION_APP_NAME}:{FIRST_S3_RELATION_NAME}")
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection info to connect to the S3 endpoint.
    connection_info = get_connection_info(juju, APPLICATION_APP_NAME, FIRST_S3_RELATION_NAME)
    # Get connection info from relation and check their correctness.
    assert connection_info["access-key"] == "test-access-key"
    assert connection_info["secret-key"] == "test-secret-key"
    assert connection_info["bucket"] == f"{APPLICATION_APP_NAME}_first_bucket"


def test_two_applications_doesnt_share_the_same_relation_data(
    juju: JujuFixture, application_s3_charm
):
    """Test that two applications connect to the s3 provider with different credentials."""
    # Set some variables to use in this test.
    another_application_app_name = "another-app"
    all_app_names = [another_application_app_name]
    all_app_names.extend(APP_NAMES)

    # Deploy another application.
    juju.ext.model.deploy(
        application_s3_charm, application_name=another_application_app_name, series="jammy"
    )
    juju.ext.model.wait_for_idle(apps=[S3_APP_NAME, APPLICATION_APP_NAME], status="active")
    juju.ext.model.wait_for_idle(apps=[another_application_app_name], status="waiting")
    # Relate the new application with the s3 provider
    # and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        f"{another_application_app_name}:{FIRST_S3_RELATION_NAME}", S3_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=all_app_names, status="active")

    # Assert the two applications have different relation (connection) data.
    application_connection_info = get_connection_info(
        juju, APPLICATION_APP_NAME, FIRST_S3_RELATION_NAME
    )
    another_application_connection_info = get_connection_info(
        juju, another_application_app_name, FIRST_S3_RELATION_NAME
    )

    assert application_connection_info != another_application_connection_info


def test_an_application_can_request_multiple_s3_providers(juju: JujuFixture):
    """Test that an application can request additional s3 credentials using the same interface."""
    # Relate the charms using another relation and wait for them exchanging some connection data.
    juju.ext.model.add_relation(f"{APPLICATION_APP_NAME}:{SECOND_S3_RELATION_NAME}", S3_APP_NAME)
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection infos from the two different relations.
    first_s3_connection_info = get_connection_info(
        juju, APPLICATION_APP_NAME, FIRST_S3_RELATION_NAME
    )
    second_s3_connection_info = get_connection_info(
        juju, APPLICATION_APP_NAME, SECOND_S3_RELATION_NAME
    )

    # Assert the two applications have different relation (connection) data.
    assert first_s3_connection_info != second_s3_connection_info
