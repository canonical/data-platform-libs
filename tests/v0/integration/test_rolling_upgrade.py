#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from pathlib import Path

import pytest
import yaml
from jubilant_adapters import JujuFixture, gather

from .helpers import (
    get_application_relation_data,
    get_leader_id,
    get_secret_by_label,
)

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "application"
DATABASE_APP_NAME = "database"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/v0/integration/database-charm/metadata.yaml").read_text()
)
DB_FIRST_DATABASE_RELATION_NAME = "first-database-db"
DB_SECOND_DATABASE_RELATION_NAME = "second-database-db"

SECRET_REF_PREFIX = "secret-"


def downgrade_to_databag(juju, app_name):
    """Helper function simulating a "rolling downgrade".

    The data_interfaces module is replaced "on-the-fly" by an older version, where Juju Secrets aren't enabled yet.
    """
    for unit in juju.ext.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        complete_command = (
            "scp tests/v0/integration/data/data_interfaces.py "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/lib/charms/data_platform_libs/v0/"
        )
        x, stdout, y = juju.juju(*complete_command.split())


def upgrade_to_secrets(juju, app_name):
    """Helper function simulating a "rolling upgrade".

    The data_interfaces module is replaced "on-the-fly" by the latest version, using Juju secrets.
    """
    for unit in juju.ext.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        complete_command = (
            "scp lib/charms/data_platform_libs/v0/data_interfaces.py "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/lib/charms/data_platform_libs/v0/"
        )
        x, stdout, y = juju.juju(*complete_command.split())


def test_deploy_charms(
    juju: JujuFixture, application_charm, database_charm, dp_libs_ubuntu_series
):
    """Deploy both charms (application and database) to use in the tests."""
    gather(
        juju.ext.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_NAME,
            num_units=1,
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
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
    )
    juju.ext.model.wait_for_idle(
        apps=[APPLICATION_APP_NAME, DATABASE_APP_NAME],
        status="active",
        wait_for_exact_units=1,
    )


# -----------------------------------------------------
# Testing 'Peer Relation'
# -----------------------------------------------------


@pytest.mark.parametrize("component", ["app", "unit"])
def test_peer_relation(component, juju: JujuFixture):
    """Relating Requires (databag only) with Provides (that could use secrets).

    This should fall back to databag usage.
    """
    downgrade_to_databag(juju, DATABASE_APP_NAME)

    # Setting and verifying two fields (one that should be a secret, one plain text)
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    unit_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "monitor-password", "value": "blablabla"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "not-a-secret", "value": "plain text"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "plain text"

    # Upgrade
    upgrade_to_secrets(juju, DATABASE_APP_NAME)

    # Both secret and databag content can be modified
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "monitor-password", "value": "blablabla_new"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field",
        **{"component": component, "field": "not-a-secret", "value": "even more plain text"},
    )
    action.wait()

    # ...and secret is moved away from the databag
    assert not (
        get_application_relation_data(
            juju,
            DATABASE_APP_NAME,
            "database-peers",
            "monitor-password",
            app_or_unit=component,
        )
    )

    assert (
        get_application_relation_data(
            juju, DATABASE_APP_NAME, "database-peers", "not-a-secret", app_or_unit=component
        )
    ) == "even more plain text"

    assert not (
        get_application_relation_data(
            juju, DATABASE_APP_NAME, "database-peers", "internal-secret", app_or_unit=component
        )
    )

    secret = get_secret_by_label(juju, f"database-peers.database.{component}")
    assert secret.get("monitor-password") == "blablabla_new"

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla_new"

    upgrade_to_secrets(juju, DATABASE_APP_NAME)

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "even more plain text"

    # Removing both secret and databag content can be modified
    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "delete-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()

    # ...successfully
    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert not action.results.get("value")

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert not action.results.get("value")


#
# NOTE: Tests below have follow a strict sequence.
# Pls only insert among if strongly justified.
#

# -----------------------------------------------------
# Testing 'Requires - databag' vs. 'Provides - secrets'
# -----------------------------------------------------


def test_unbalanced_versions_falling_back_to_databag_req_databag_vs_prov_secrets(
    juju: JujuFixture,
):
    """Relating Requires (databag only) with Provides (that could use secrets).

    This should fall back to databag usage.
    """
    downgrade_to_databag(juju, APPLICATION_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Secrest are correctly set in databag
    username = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "username"
    )
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "password"
    )

    assert username
    assert password

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    action.wait()
    assert action.results.get("value") == username

    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password


def test_transparent_upgrade_requires_databag_vs_provides_secrets(juju: JujuFixture):
    """Upgrading Requires to use secrets (if possible).

    YET, already existing relations keep using the databag for all operations (fetch, update, delete)
    """
    upgrade_to_secrets(juju, APPLICATION_APP_NAME)

    # Application charm leader
    leader_app_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-user')
    uris_new_val = "http://username:password@example.com"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.first_database_relation.id,
            "field": "uris",
            "value": uris_new_val,
        },
    )
    action.wait()

    # Relation data is correct
    assert uris_new_val == get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "uris"
    )
    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert action.results.get("value") == uris_new_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert action.results.get("value") == uris_new_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    action.wait()

    # Relation data is correct
    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "uris"
        )
        is None
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert not action.results.get("value")


def test_transparent_upgrade_requires_databag_vs_provides_secrets_upgrade_new_secret(
    juju: JujuFixture,
):
    """After Requires upgrade, we stay on the databag."""
    # Application charm leader
    leader_app_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-tls')
    tls_ca_val = "<ca_cert_here>"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.first_database_relation.id,
            "field": "tls-ca",
            "value": tls_ca_val,
        },
    )
    action.wait()

    # We stay on the databag
    assert get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "tls-ca"
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "secret-tls"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    action.wait()
    assert action.results.get("value") == tls_ca_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    action.wait()
    assert action.results.get("value") == tls_ca_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    action.wait()

    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "tls-ca"},
    )
    action.wait()
    assert not action.results.get("value")

    # Previously existing sensitive data stays where it was
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "password"
    )
    assert password

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password


# -----------------------------------------------------
# Testing 'Requires - secrets' vs. 'Provides - databag'
# -----------------------------------------------------


def test_unbalanced_versions_falling_back_to_databag_req_secrets_vs_prov_databag(
    juju: JujuFixture,
):
    """Relating Requires (secrets available) with Provides (databag only).

    This should fall back to databag usage.
    """
    downgrade_to_databag(juju, DATABASE_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.second_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Relation data is correct
    username = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "username"
    )
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "password"
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "username"},
    )
    action.wait()
    assert action.results.get("value") == username

    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password


def test_transparent_upgrade_keeping_databag_requires_secrets_vs_provides_databag(
    juju: JujuFixture,
):
    """Upgrading Provides to use secrets (if possible).

    YET, already existing relations keep using the databag for all operations (fetch, update, delete)
    """
    upgrade_to_secrets(juju, DATABASE_APP_NAME)

    # Application charm leader
    leader_app_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-user')
    uris_new_val = "http://username:password@example.com"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "uris",
            "value": uris_new_val,
        },
    )
    action.wait()

    # Relation data is correct
    assert uris_new_val == get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "uris"
    )
    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert action.results.get("value") == uris_new_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert action.results.get("value") == uris_new_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    action.wait()

    # Relation data is correct
    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "uris"
        )
        is None
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert not action.results.get("value")


def test_transparent_upgrade_keeping_databag_requires_secrets_vs_provides_databag_upgrade_new_secret(
    juju: JujuFixture,
):
    """After Provider upgrade, the relation remains on databag."""
    # Application charm leader
    leader_app_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"

    # DB leader
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"

    # Set new sensitive information (if secrets: stored in 'secret-tls')
    tls_ca_val = "<ca_cert_here>"
    action = juju.ext.model.units.get(leader_name).run_action(
        "set-relation-field",
        **{
            "relation_id": pytest.second_database_relation.id,
            "field": "tls-ca",
            "value": tls_ca_val,
        },
    )
    action.wait()

    tls_ca_val = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "tls-ca"
    )

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "secret-tls"
        )
        is None
    )

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    action.wait()
    assert action.results.get("value") == tls_ca_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    action.wait()
    assert action.results.get("value") == tls_ca_val

    action = juju.ext.model.units.get(leader_name).run_action(
        "delete-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    action.wait()

    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "tls-ca"},
    )
    action.wait()
    assert not action.results.get("value")

    # Previously existing sensitive data stays where it was
    password = get_application_relation_data(
        juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "password"
    )
    assert password

    assert (
        get_application_relation_data(
            juju, APPLICATION_APP_NAME, DB_SECOND_DATABASE_RELATION_NAME, "secret-user"
        )
        is None
    )

    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password
