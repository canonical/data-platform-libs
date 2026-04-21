#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
from pathlib import Path
from time import sleep

import psycopg2
import pytest
import yaml
from jubilant_adapters import JujuFixture, gather

from .helpers import (
    build_connection_string,
    check_logs,
    get_application_relation_data,
    get_juju_secret,
    get_leader_id,
    get_non_leader_id,
    get_secret_by_label,
    get_secret_revision_by_label,
    list_juju_secrets,
)

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
DATABASE_DUMMY_APP_NAME = "dummy-database"
ANOTHER_DATABASE_APP_NAME = "another-database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME, ANOTHER_DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/v0/integration/database-charm/metadata.yaml").read_text()
)
DATABASE_DUMMY_APP_METADATA = yaml.safe_load(
    Path("./tests/v0/integration/dummy-database-charm/metadata.yaml").read_text()
)

DB_FIRST_DATABASE_RELATION_NAME = "first-database-db"
DB_SECOND_DATABASE_RELATION_NAME = "second-database-db"
ROLES_FIRST_DATABASE_RELATION_NAME = "first-database-roles"
USERNAME_FIRST_DATABASE_RELATION_NAME = "first-database-username"
PREFIX_DATABASE_RELATION_NAME = "database-with-prefix"

MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "multiple-database-clusters"
ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "aliased-multiple-database-clusters"

SECRET_REF_PREFIX = "secret-"

NUM_DB = 3
NUM_DUMMY_DB = 2
NUM_OTHER_DB = 1
NUM_APP = 2


def test_deploy_charms(
    juju: JujuFixture,
    application_charm,
    database_charm,
    dummy_database_charm,
    dp_libs_ubuntu_series,
):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    gather(
        juju.ext.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=NUM_APP,
            series=dp_libs_ubuntu_series,
        ),
        juju.ext.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=DATABASE_APP_NAME,
            num_units=NUM_DB,
            series=dp_libs_ubuntu_series,
        ),
        juju.ext.model.deploy(
            dummy_database_charm,
            resources={
                "database-image": DATABASE_DUMMY_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=DATABASE_DUMMY_APP_NAME,
            num_units=NUM_DUMMY_DB,
            series=dp_libs_ubuntu_series,
        ),
        juju.ext.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"][
                    "upstream-source"
                ]
            },
            application_name=ANOTHER_DATABASE_APP_NAME,
            series=dp_libs_ubuntu_series,
        ),
    )

    gather(
        juju.ext.model.wait_for_idle(
            apps=[APPLICATION_APP_NAME], status="active", wait_for_exact_units=NUM_APP
        ),
        juju.ext.model.wait_for_idle(
            apps=[DATABASE_APP_NAME], status="active", wait_for_exact_units=NUM_DB
        ),
        juju.ext.model.wait_for_idle(
            apps=[DATABASE_DUMMY_APP_NAME], status="active", wait_for_exact_units=NUM_DUMMY_DB
        ),
        juju.ext.model.wait_for_idle(
            apps=[ANOTHER_DATABASE_APP_NAME], status="active", wait_for_exact_units=NUM_OTHER_DB
        ),
    )


@pytest.mark.usefixtures("only_without_juju_secrets")
@pytest.mark.parametrize("component", ["app", "unit"])
def test_peer_relation(component, juju: JujuFixture):
    """Testing peer relation using the DataPeer class."""
    # Setting and verifying two secret fields
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "monitor-password", "value": "blablabla"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "secret-field", "value": "blablabla2"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "secret-field"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla2"

    # Setting and verifying a non-secret field
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "not-a-secret", "value": "plain text"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "plain text"

    # Deleting all fields
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert not action.results.get("value")

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert not action.results.get("value")

    assert not (
        get_application_relation_data(juju, DATABASE_APP_NAME, "database-peers", "not-a-secret")
    )


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("component", ["app", "unit"])
def test_peer_relation_secrets(component, juju: JujuFixture):
    """Testing peer relation using the DataPeer class."""
    # Setting and verifying two secret fields
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Generally we shouldn't have test decision based on pytest.mark.parametrize
    # but I think this is a valid exception
    owner = "database" if component == "app" else unit_name

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "monitor-password", "value": "blablabla"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "secret-field", "value": "blablabla2"},
    )
    action.wait()

    secret = get_secret_by_label(juju, f"database-peers.database.{component}", owner)
    assert secret.get("monitor-password") == "blablabla"
    assert secret.get("secret-field") == "blablabla2"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "secret-field"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla2"

    # Setting and verifying a non-secret field
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "not-a-secret", "value": "plain text"},
    )
    action.wait()

    secret = get_secret_by_label(juju, f"database-peers.database.{component}", owner)
    assert not secret.get("not-a-secret")

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "plain text"

    # Deleting all fields
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()

    secret = get_secret_by_label(juju, f"database-peers.database.{component}", owner)
    assert secret.get("secret-field") == "blablabla2"
    assert secret.get("monitor-password") == "#DELETED#"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert not action.results.get("value")

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert not action.results.get("value")

    assert not (
        get_application_relation_data(
            juju, DATABASE_APP_NAME, "database-peers", "not-a-secret", app_or_unit=component
        )
    )

    # Internal secret URI is not saved on the databag
    assert not (
        get_application_relation_data(
            juju, DATABASE_APP_NAME, "database-peers", "internal-secret", app_or_unit=component
        )
    )

    # Cleanup
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-secret", **{"component": component}
    )
    action.wait()


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("component", ["app", "unit"])
def test_peer_relation_secret_revisions(component, juju: JujuFixture):
    """Check that only a content change triggers the emission of a new revision."""
    # Given
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{leader_id}"
    owner = "database" if component == "app" else unit_name

    # When
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "secret-field", "value": "blablabla"},
    )
    action.wait()

    original_secret_revision = get_secret_revision_by_label(
        juju, f"database-peers.database.{component}", owner
    )

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "secret-field", "value": "blablabla2"},
    )
    action.wait()

    changed_secret_revision = get_secret_revision_by_label(
        juju, f"database-peers.database.{component}", owner
    )

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "secret-field", "value": "blablabla2"},
    )
    action.wait()

    unchanged_secret_revision = get_secret_revision_by_label(
        juju, f"database-peers.database.{component}", owner
    )

    # Then
    assert original_secret_revision + 1 == changed_secret_revision
    assert changed_secret_revision == unchanged_secret_revision


@pytest.mark.usefixtures("only_with_juju_secrets")
@pytest.mark.parametrize("component", ["app", "unit"])
def test_peer_relation_set_secret(component, juju: JujuFixture):
    """Testing peer relation using the DataPeer class."""
    # Setting and verifying two secret fields
    leader_id = get_leader_id(juju, DATABASE_DUMMY_APP_NAME)
    unit_name = f"{DATABASE_DUMMY_APP_NAME}/{leader_id}"

    # Generally we shouldn't have test decision based on pytest.mark.parametrize
    # but I think this is a valid exception
    owner = "dummy-database" if component == "app" else unit_name

    # Setting a new secret field dynamically
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-secret",
        **{"component": component, "field": "new-field", "value": "blablabla"},
    )
    action.wait()

    secret = get_secret_by_label(juju, f"database-peers.dummy-database.{component}", owner)
    assert secret.get("new-field") == "blablabla"

    # Setting a new secret field dynamically in a new, dedicated secret
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-secret",
        **{
            "component": component,
            "field": "mygroup-field1",
            "value": "blablabla3",
            "group": "mygroup",
        },
    )
    action.wait()
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-secret",
        **{
            "component": component,
            "field": "mygroup-field2",
            "value": "blablabla4",
            "group": "mygroup",
        },
    )
    action.wait()

    secret = get_secret_by_label(juju, f"database-peers.dummy-database.{component}.mygroup", owner)
    assert secret.get("mygroup-field1") == "blablabla3"
    assert secret.get("mygroup-field2") == "blablabla4"

    # Getting the secret
    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "new-field"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "mygroup-field1@mygroup"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla3"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "mygroup-field2@mygroup"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla4"

    # Cleanup
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-secret", **{"component": component}
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-secret", **{"component": component, "group": "mygroup"}
    )
    action.wait()


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_peer_relation_non_leader_unit_secrets(juju: JujuFixture):
    """Testing peer relation using the DataPeer class."""
    # Setting and verifying two secret fields
    non_leader_unit_id = get_non_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{non_leader_unit_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": "unit", "field": "monitor-password", "value": "blablabla"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": "unit", "field": "secret-field", "value": "blablabla2"},
    )
    action.wait()

    secret = get_secret_by_label(juju, "database-peers.database.unit", unit_name)
    assert secret.get("monitor-password") == "blablabla"
    assert secret.get("secret-field") == "blablabla2"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": "unit", "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": "unit", "field": "secret-field"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla2"

    # Setting and verifying a non-secret field
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": "unit", "field": "not-a-secret", "value": "plain text"},
    )
    action.wait()

    secret = get_secret_by_label(juju, "database-peers.database.unit", unit_name)
    assert not secret.get("not-a-secret")

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": "unit", "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "plain text"

    # Deleting all fields
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": "unit", "field": "monitor-password"}
    )
    action.wait()

    secret = get_secret_by_label(juju, "database-peers.database.unit", unit_name)
    assert secret.get("secret-field") == "blablabla2"
    assert secret.get("monitor-password") == "#DELETED#"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": "unit", "field": "monitor-password"}
    )
    action.wait()
    assert not action.results.get("value")

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": "unit", "field": "not-a-secret"}
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": "unit", "field": "not-a-secret"}
    )
    action.wait()
    assert not action.results.get("value")

    assert not (
        get_application_relation_data(
            juju, DATABASE_APP_NAME, "database-peers", "not-a-secret", app_or_unit="unit"
        )
    )

    # Internal secret URI is not saved on the databag
    assert not (
        get_application_relation_data(
            juju, DATABASE_APP_NAME, "database-peers", "internal-secret", app_or_unit="unit"
        )
    )

    # Cleanup
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-secret", **{"component": "unit"}
    )
    action.wait()


def test_peer_relation_non_leader_can_read_app_data(juju: JujuFixture):
    """Testing peer relation using the DataPeer class."""
    # Setting and verifying two secret fields
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": "app", "field": "monitor-password", "value": "blablabla"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": "app", "field": "not-a-secret", "value": "plain text"},
    )
    action.wait()

    # Checking that non-leader unit can fetch any (i.e. also app) secret
    non_leader_unit_id = get_non_leader_id(juju, DATABASE_APP_NAME)
    non_leader_unit_name = f"{DATABASE_APP_NAME}/{non_leader_unit_id}"
    action = juju.ext.model.units.get(non_leader_unit_name).run_action(
        "get-peer-relation-field", **{"component": "app", "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla"

    action = juju.ext.model.units.get(non_leader_unit_name).run_action(
        "get-peer-relation-field", **{"component": "app", "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "plain text"


def test_other_peer_relation(juju: JujuFixture):
    """Testing peer relation using the DataPeer class."""
    # Setting and verifying two secret fields
    component = "unit"
    units = juju.ext.model.applications[DATABASE_APP_NAME].units
    for unit in units:
        action = unit.run_action(
            "set-peer-relation-field",
            **{"component": component, "field": "monitor-password", "value": "blablabla"},
        )
        action.wait()

        action = unit.run_action(
            "set-peer-relation-field",
            **{"component": component, "field": "non-secret-field", "value": "blablabla2"},
        )
        action.wait()

    for main_unit in units:
        action = main_unit.run_action(
            "get-other-peer-relation-field", **{"field": "monitor-password"}
        )
        action.wait()

        for unit in units:
            if unit != main_unit:
                assert action.results.get(unit.name.replace("/", "-")) == "blablabla"

        action = main_unit.run_action(
            "get-other-peer-relation-field", **{"field": "non-secret-field"}
        )
        action.wait()

        for unit in units:
            if unit != main_unit:
                assert action.results.get(unit.name.replace("/", "-")) == "blablabla2"


def test_other_peer_relation_scale(juju: JujuFixture):
    """The scaling test is the 'continuation' of the previous (test_other_peer_relation()) test.

    We assume data set up there.
    """
    juju.ext.model.applications[DATABASE_APP_NAME].scale(scale_change=-1)
    juju.ext.model.wait_for_idle(apps=[DATABASE_APP_NAME], status="active", wait_for_exact_units=2)
    units = juju.ext.model.applications[DATABASE_APP_NAME].units

    for main_unit in units:
        action = main_unit.run_action(
            "get-other-peer-relation-field", **{"field": "monitor-password"}
        )
        action.wait()

        for unit in units:
            if unit != main_unit:
                assert action.results.get(unit.name.replace("/", "-")) == "blablabla"

        action = main_unit.run_action(
            "get-other-peer-relation-field", **{"field": "non-secret-field"}
        )
        action.wait()

        for unit in units:
            if unit != main_unit:
                assert action.results.get(unit.name.replace("/", "-")) == "blablabla2"

    juju.ext.model.applications[DATABASE_APP_NAME].add_units(count=1)
    juju.ext.model.wait_for_idle(apps=[DATABASE_APP_NAME], status="active", wait_for_exact_units=3)
    new_units = juju.ext.model.applications[DATABASE_APP_NAME].units
    unit = list(set(new_units) - set(units))[0]

    for main_unit in units:
        action = main_unit.run_action(
            "get-other-peer-relation-field", **{"field": "monitor-password"}
        )
        action.wait()

        assert action.results.get(unit) is None

        action = main_unit.run_action(
            "get-other-peer-relation-field", **{"field": "non-secret-field"}
        )
        action.wait()

        assert action.results.get(unit) is None


def test_database_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of database relation interface."""
    # Relate the charms and wait for them exchanging some connection data.

    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection string to connect to the database.
    connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME
    )

    # Connect to the database.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that it's possible to write and read data from the database that
        # was created for the application.
        connection.autocommit = True
        cursor.execute("DROP TABLE IF EXISTS test;")
        cursor.execute("CREATE TABLE test(data TEXT);")
        cursor.execute("INSERT INTO test(data) VALUES('some data');")
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

        # Check the version that the application received is the same on the database server.
        cursor.execute("SELECT version();")
        data = cursor.fetchone()

        # Get the version of the database and compare with the information that
        # was retrieved directly from the database.
        version = get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "version"
        )
        assert version == data[0]


def test_user_with_extra_roles(juju: JujuFixture):
    """Test superuser actions and the request for more permissions."""
    # Get the connection string to connect to the database.
    connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME
    )

    # Connect to the database.
    connection = psycopg2.connect(connection_string)
    connection.autocommit = True
    cursor = connection.cursor()

    # Test the user can create a database and another user.
    cursor.execute("CREATE DATABASE another_database;")
    cursor.execute("CREATE USER another_user WITH ENCRYPTED PASSWORD 'test-password';")

    cursor.close()
    connection.close()


def test_postgresql_plugin(juju: JujuFixture):
    """Test that the application charm can check whether a plugin is enabled."""
    # Check that the plugin is disabled.
    unit_name = f"{APPLICATION_APP_NAME}/0"
    action = juju.ext.model.units.get(unit_name).run_action(
        "get-plugin-status", **{"plugin": "citext"}
    )
    action.wait()
    assert action.results.get("plugin-status") == "disabled"

    # Connect to the database and enable the plugin (PostgreSQL extension).
    connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME
    )
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        connection.autocommit = True
        cursor.execute("CREATE EXTENSION citext;")
    connection.close()

    # Check that the plugin is enabled.
    action = juju.ext.model.units.get(unit_name).run_action(
        "get-plugin-status", **{"plugin": "citext"}
    )
    action.wait()
    assert action.results.get("plugin-status") == "enabled"


def test_two_applications_dont_share_the_same_relation_data(juju: JujuFixture, application_charm):
    """Test that two different application connect to the database with different credentials."""
    # Set some variables to use in this test.
    another_application_app_name = "another-application"
    all_app_names = [another_application_app_name]
    all_app_names.extend(APP_NAMES)

    # Deploy another application.
    juju.ext.model.deploy(
        application_charm, application_name=another_application_app_name, series="jammy"
    )
    juju.ext.model.wait_for_idle(apps=all_app_names, status="active")

    # Relate the new application with the database
    # and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        f"{another_application_app_name}:{DB_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=all_app_names, status="active")

    # Assert the two application have different relation (connection) data.
    application_connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME
    )
    another_application_connection_string = build_connection_string(
        juju, another_application_app_name, DB_FIRST_DATABASE_RELATION_NAME
    )
    assert application_connection_string != another_application_connection_string


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_databag_usage_correct(juju: JujuFixture, application_charm):
    for field in ["username", "password"]:
        assert get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, field
        )


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_secrets_usage_correct_secrets(juju: JujuFixture, application_charm):
    for field in ["username", "password", "uris"]:
        assert (
            get_application_relation_data(
                juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, field
            )
            is None
        )
    assert get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "secret-user"
    )


@pytest.mark.usefixtures("only_without_juju_secrets")
def test_database_roles_relation_with_charm_libraries(juju: JujuFixture):
    """Test basic functionality of database-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.

    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{ROLES_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    entity_name = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_FIRST_DATABASE_RELATION_NAME, "entity-name"
    )
    entity_pass = get_application_relation_data(
        juju, APPLICATION_APP_NAME, ROLES_FIRST_DATABASE_RELATION_NAME, "entity-password"
    )

    assert entity_name is not None
    assert entity_pass is not None


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_database_roles_relation_with_charm_libraries_secrets(juju: JujuFixture):
    """Test basic functionality of database-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.

    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{ROLES_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    secret_uri = get_application_relation_data(
        juju,
        APPLICATION_APP_NAME,
        ROLES_FIRST_DATABASE_RELATION_NAME,
        f"{SECRET_REF_PREFIX}entity",
    )

    secret_content = get_juju_secret(juju, secret_uri)
    entity_name = secret_content["entity-name"]
    entity_pass = secret_content["entity-password"]

    assert entity_name is not None
    assert entity_pass is not None


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_database_username(juju: JujuFixture):
    """Test basic functionality of database-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.

    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{USERNAME_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    secret_uri = get_application_relation_data(
        juju,
        APPLICATION_APP_NAME,
        USERNAME_FIRST_DATABASE_RELATION_NAME,
        "secret-user",
    )

    secret_content = get_juju_secret(juju, secret_uri)
    name = secret_content["username"]
    password = secret_content["password"]

    assert name == "testuser"
    assert password is not None


def test_database_prefix(juju: JujuFixture):
    """Test basic functionality of database-roles relation interface."""
    # Relate the charms and wait for them exchanging some connection data.

    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{PREFIX_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    assert (
        get_application_relation_data(
            juju,
            APPLICATION_APP_NAME,
            PREFIX_DATABASE_RELATION_NAME,
            "prefix-databases",
        )
        == "testdb1,testdb2"
    )

    data = json.loads(
        get_application_relation_data(
            juju,
            APPLICATION_APP_NAME,
            PREFIX_DATABASE_RELATION_NAME,
            "data",
        )
    )
    assert data["prefix-matching"] == "all"


def test_an_application_can_connect_to_multiple_database_clusters(
    juju: JujuFixture, database_charm
):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    first_cluster_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}", DATABASE_APP_NAME
    )
    # This call enables the unit to be available in the relation changed event.
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    second_cluster_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_id=first_cluster_relation.id,
    )
    another_application_connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_id=second_cluster_relation.id,
    )
    assert application_connection_string != another_application_connection_string


def test_an_application_can_connect_to_multiple_aliased_database_clusters(
    juju: JujuFixture, database_charm
):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        DATABASE_APP_NAME,
    )
    # This call enables the unit to be available in the relation changed event.
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_alias="cluster1",
    )
    another_application_connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        relation_alias="cluster2",
    )
    assert application_connection_string != another_application_connection_string


def test_an_application_can_request_multiple_databases(juju: JujuFixture, application_charm):
    """Test that an application can request additional databases using the same interface."""
    # Relate the charms using another relation and wait for them exchanging some connection data.
    sleep(5)
    pytest.second_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Get the connection strings to connect to both databases.
    first_database_connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME
    )
    second_database_connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME
    )

    # Assert the two application have different relation (connection) data.
    assert first_database_connection_string != second_database_connection_string


def test_external_node_connectivity_field(juju: JujuFixture, application_charm):
    # Check that the flag is missing if not requested
    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            "database",
            "external-node-connectivity",
            related_endpoint=DB_FIRST_DATABASE_RELATION_NAME,
        )
    ) is None

    # Check that the second relation raises the flag
    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            "database",
            "external-node-connectivity",
            related_endpoint=DB_SECOND_DATABASE_RELATION_NAME,
        )
    ) == "true"


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_provider_with_additional_secrets(juju: JujuFixture, database_charm):
    # Let's make sure that there was enough time for the relation initialization to communicate secrets
    secret_fields = get_application_relation_data(
        juju,
        DATABASE_APP_NAME,
        DATABASE_APP_NAME,
        "requested-secrets",
        related_endpoint=DB_SECOND_DATABASE_RELATION_NAME,
    )
    assert {"topsecret", "donttellanyone"} <= set(json.loads(secret_fields))

    # Set secret
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-secret", **{"relation_id": pytest.second_database_relation.id, "field": "topsecret"}
    )
    action.wait()

    # Get secret original value
    secret_uri = get_application_relation_data(
        juju,
        APPLICATION_APP_NAME,
        DB_SECOND_DATABASE_RELATION_NAME,
        f"{SECRET_REF_PREFIX}extra",
    )

    secret_content = get_juju_secret(juju, secret_uri)
    topsecret1 = secret_content["topsecret"]

    # Re-set secret
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-secret", **{"relation_id": pytest.second_database_relation.id, "field": "topsecret"}
    )
    action.wait()

    # Get secret after change
    secret_uri = get_application_relation_data(
        juju,
        APPLICATION_APP_NAME,
        DB_SECOND_DATABASE_RELATION_NAME,
        f"{SECRET_REF_PREFIX}extra",
    )

    secret_content = get_juju_secret(juju, secret_uri)
    topsecret2 = secret_content["topsecret"]

    assert topsecret1 != topsecret2


@pytest.mark.usefixtures("only_with_juju_secrets")
def test_relation_secret_revisions(juju: JujuFixture):
    """Check that only a content change triggers the emission of a new revision."""
    # Given
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    owner = "database"
    rel_id = pytest.second_database_relation.id
    group_mapping = "extra"

    # When
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-secret", **{"relation_id": rel_id, "field": "topsecret", "value": "initialvalue"}
    )
    action.wait()

    original_secret_revision = get_secret_revision_by_label(
        juju, f"{DATABASE_APP_NAME}.{rel_id}.{group_mapping}.secret", owner
    )

    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "topsecret",
            "value": "changedvalue",
        },
    )
    action.wait()

    changed_secret_revision = get_secret_revision_by_label(
        juju, f"{DATABASE_APP_NAME}.{rel_id}.{group_mapping}.secret", owner
    )

    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "topsecret",
            "value": "changedvalue",
        },
    )
    action.wait()

    unchanged_secret_revision = get_secret_revision_by_label(
        juju, f"{DATABASE_APP_NAME}.{rel_id}.{group_mapping}.secret", owner
    )

    # Then
    assert original_secret_revision + 1 == changed_secret_revision
    assert changed_secret_revision == unchanged_secret_revision


@pytest.mark.parametrize("field,value", [("new_field", "blah"), ("tls", "True")])
@pytest.mark.usefixtures("only_without_juju_secrets")
def test_provider_get_set_delete_fields(field, value, juju: JujuFixture):
    # Add normal field
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, field
        )
        == value
    )

    # Check all application units can read remote relation data
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        action = unit.run_action(
            "get-relation-field",
            **{
                "relation_id": pytest.second_database_relation.id,
                "field": field,
            },
        )
        action.wait()
        assert action.results.get("value") == value

    # Check if database can retrieve self-side relation data
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    action.wait()
    assert action.results.get("value") == value

    # Delete normal field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": field},
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, field
        )
        is None
    )

    # Delete non-existent field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "doesnt_exist"},
    )
    action.wait()
    # Juju2 syntax
    assert int(action.results["Code"]) == 0
    assert check_logs(
        juju,
        strings=["Non-existing field 'doesnt_exist' was attempted to be removed from the databag"],
    )


@pytest.mark.log_errors_allowed(
    "Non-existing field 'doesnt_exist' was attempted to be removed from the databag"
)
@pytest.mark.parametrize(
    "field,value,relation_field",
    [
        ("new_field", "blah", "new_field"),
        ("tls", "True", "secret-tls"),
    ],
)
@pytest.mark.usefixtures("only_with_juju_secrets")
def test_provider_get_set_delete_fields_secrets(field, value, relation_field, juju: JujuFixture):
    # Add field
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    action.wait()

    assert get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, relation_field
    )

    # Check all application units can read remote relation data
    for unit in juju.ext.model.applications[APPLICATION_APP_NAME].units:
        action = unit.run_action(
            "get-relation-field",
            **{
                "relation_id": pytest.second_database_relation.id,
                "field": field,
            },
        )
        action.wait()
        assert action.results.get("value") == value

    # Check if database can retrieve self-side relation data
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    action.wait()
    assert action.results.get("value") == value

    # Delete field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": field},
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, relation_field
        )
        is None
    )

    # Check that the field is deleted
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
        },
    )
    action.wait()
    assert not action.results.get("value")

    # Delete non-existent notmal and secret field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "doesnt_exist"},
    )
    action.wait()
    assert action.results["return-code"] == 0


@pytest.mark.log_errors_allowed("Can't delete secret for relation")
@pytest.mark.usefixtures("only_with_juju_secrets")
def test_provider_deleted_secret_is_removed(juju: JujuFixture):
    """The 'tls' field, that was removed in the previous test has it's secret removed."""
    # Add field
    field = "tls"
    value = "True"
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": field,
            "value": value,
        },
    )
    action.wait()

    # Get TLS secret pointer
    secret_uri = get_application_relation_data(
        juju,
        APPLICATION_APP_NAME,
        DB_SECOND_DATABASE_RELATION_NAME,
        f"{SECRET_REF_PREFIX}{field}",
    )

    # Delete field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": field},
    )
    action.wait()
    assert not (
        check_logs(
            juju,
            strings=["Non-existing field 'tls' was attempted to be removed from the databag"],
        )
    )
    assert not (check_logs(juju, strings=["Can't delete secret for relation"]))

    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": field},
    )
    action.wait()
    assert check_logs(
        juju, strings=["Non-existing field 'tls' was attempted to be removed from the databag"]
    )
    assert check_logs(juju, strings=["Can't delete secret for relation"])

    assert (
        get_application_relation_data(
            juju,
            APPLICATION_APP_NAME,
            DB_SECOND_DATABASE_RELATION_NAME,
            f"{SECRET_REF_PREFIX}{field}",
        )
        is None
    )

    secrets = list_juju_secrets(juju)
    secret_xid = secret_uri.split("/")[-1]
    assert secret_xid not in secrets


def test_requires_get_set_delete_fields(juju: JujuFixture):
    # Add normal field
    leader_id = get_leader_id(juju, APPLICATION_APP_NAME)
    leader_name = f"{APPLICATION_APP_NAME}/{leader_id}"

    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field",
            "value": "blah",
        },
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            DB_SECOND_DATABASE_RELATION_NAME,
            "new_field",
            related_endpoint="second-database-db",
        )
        == "blah"
    )

    # Check all application units can read remote relation data
    for unit in juju.ext.model.applications[DATABASE_APP_NAME].units:
        action = unit.run_action(
            "get-relation-field",
            **{
                "relation_id": pytest.second_database_relation.id,
                "field": "new_field",
            },
        )
        action.wait()
        assert action.results.get("value") == "blah"

    # Check if database can retrieve self-side relation data
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field",
            "value": "blah",
        },
    )
    action.wait()
    assert action.results.get("value") == "blah"

    # Delete normal field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field"},
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            DB_SECOND_DATABASE_RELATION_NAME,
            "new_field",
            related_endpoint="second-database-db",
        )
        is None
    )


@pytest.mark.log_errors_allowed(
    "This operation (update_relation_data()) can only be performed by the leader unit"
)
@pytest.mark.log_errors_allowed(
    "This operation (delete_relation_data()) can only be performed by the leader unit"
)
def test_provider_set_delete_fields_leader_only(juju: JujuFixture):
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field",
            "value": "blah",
        },
    )
    action.wait()

    unit_id = get_non_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{unit_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field2",
            "value": "blah2",
        },
    )
    action.wait()
    assert check_logs(
        juju,
        strings=[
            "This operation (update_relation_data()) can only be performed by the leader unit"
        ],
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "new_field2"
        )
        is None
    )

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field"},
    )
    action.wait()
    assert check_logs(
        juju,
        strings=[
            "This operation (delete_relation_data()) can only be performed by the leader unit"
        ],
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "new_field"
        )
        == "blah"
    )


def test_requires_set_delete_fields(juju: JujuFixture):
    # Add field
    leader_id = get_leader_id(juju, APPLICATION_APP_NAME)
    leader_name = f"{APPLICATION_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field_req",
            "value": "blah-req",
        },
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field_req",
            related_endpoint=DB_SECOND_DATABASE_RELATION_NAME,
        )
        == "blah-req"
    )

    # Delete field
    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field_req"},
    )
    action.wait()

    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field_req",
            related_endpoint=DB_SECOND_DATABASE_RELATION_NAME,
        )
        is None
    )


@pytest.mark.log_errors_allowed(
    "This operation (update_relation_data()) can only be performed by the leader unit"
)
@pytest.mark.log_errors_allowed(
    "This operation (delete_relation_data()) can only be performed by the leader unit"
)
def test_requires_set_delete_fields_leader_only(juju: JujuFixture):
    leader_id = get_leader_id(juju, APPLICATION_APP_NAME)
    leader_name = f"{APPLICATION_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field-req",
            "value": "blah-req",
        },
    )
    action.wait()

    unit_id = get_non_leader_id(juju, APPLICATION_APP_NAME)
    unit_name = f"{APPLICATION_APP_NAME}/{unit_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "new_field2-req",
            "value": "blah2-req",
        },
    )
    action.wait()
    assert check_logs(
        juju,
        strings=[
            "This operation (update_relation_data()) can only be performed by the leader unit"
        ],
    )

    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field2-req",
            related_endpoint=DB_SECOND_DATABASE_RELATION_NAME,
        )
        is None
    )

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "new_field-req"},
    )
    action.wait()
    assert check_logs(
        juju,
        strings=[
            "This operation (delete_relation_data()) can only be performed by the leader unit"
        ],
    )

    assert (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            DATABASE_APP_NAME,
            "new_field-req",
            related_endpoint=DB_SECOND_DATABASE_RELATION_NAME,
        )
        == "blah-req"
    )


def test_scaling_requires_can_access_shared_secret(juju):
    """When scaling up the application, new units should have access to relation secrets."""
    juju.ext.model.applications[APPLICATION_APP_NAME].scale(3)

    juju.ext.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME], status="active", timeout=(15 * 60), wait_for_exact_units=3
    )

    old_unit_name = f"{APPLICATION_APP_NAME}/1"
    new_unit_name = f"{APPLICATION_APP_NAME}/2"

    action = juju.ext.model.units.get(old_unit_name).run_action(
        "get-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "password",
        },
    )
    action.wait()
    orig_password = action.results.get("value")

    action = juju.ext.model.units.get(new_unit_name).run_action(
        "get-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "password",
        },
    )
    action.wait()
    new_password = action.results.get("value")
    assert new_password == orig_password
