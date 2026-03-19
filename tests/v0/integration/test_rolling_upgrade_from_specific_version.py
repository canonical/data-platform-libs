#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from jubilant_adapters import JujuFixture, gather
from lib.charms.data_platform_libs.v0.data_interfaces import LIBPATCH

from .helpers import get_leader_id

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


def old_version_to_upgrade_from():
    """Determine how many versions to go back from tox environment (default: previous version)."""
    try:
        go_backwards = int(os.environ["TOX_ENV"].split("-")[-1])
    except TypeError:
        go_backwards = 1
    return LIBPATCH - go_backwards


def downgrade_to_old_version(juju, app_name):
    """Helper function simulating a "rolling downgrade".

    The data_interfaces module is replaced "on-the-fly" by an older version.
    """
    version = old_version_to_upgrade_from()
    logger.info(f"Downgrading {app_name} to version {version}")
    for unit in juju.ext.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        path = f"tests/v0/integration/data/data_interfaces.py.v{version}"

        result = subprocess.run(
            f"grep 'LIBPATCH = {version}' {path}",
            shell=True,
        )

        assert not result.returncode, "Incorrect version of data_interfaces fetched."

        complete_command = (
            f"scp {path} "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}"
            "/charm/lib/charms/data_platform_libs/v0/data_interfaces.py"
        )
        ret_code, stdout, _ = juju.juju(*complete_command.split())
        # scp was successful
        assert not ret_code, f"Couldn't perform copy to {unit.name}."


def upgrade_to_new_version(juju, app_name):
    """Helper function simulating a "rolling upgrade".

    The data_interfaces module is replaced "on-the-fly" by the latest version.
    """
    logger.info(f"Upgrading {app_name} to latest version")
    for unit in juju.ext.model.applications[app_name].units:
        unit_name_with_dash = unit.name.replace("/", "-")
        complete_command = (
            "scp lib/charms/data_platform_libs/v0/data_interfaces.py "
            f"{unit.name}:/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/lib/charms/data_platform_libs/v0/"
        )
        ret_code, stdout, _ = juju.juju(*complete_command.split())
        # scp was successful
        assert not ret_code, f"Couldn't perform copy to {unit.name}."


@pytest.mark.usefixtures("fetch_old_versions")
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
    """Peer relation safe across upgrades."""
    downgrade_to_old_version(juju, DATABASE_APP_NAME)

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
    upgrade_to_new_version(juju, DATABASE_APP_NAME)

    # Both secret and databag content can be modified -- even twice ;-)
    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field-multiple",
        **{"component": component, "field": "monitor-password", "value": "blablabla_new"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "set-peer-relation-field-multiple",
        **{"component": component, "field": "not-a-secret", "value": "even more plain text"},
    )
    action.wait()

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "monitor-password"}
    )
    action.wait()
    assert action.results.get("value") == "blablabla_new2"

    upgrade_to_new_version(juju, DATABASE_APP_NAME)

    action = juju.ext.model.units.get(unit_name).run_action(
        "get-peer-relation-field", **{"component": component, "field": "not-a-secret"}
    )
    action.wait()
    assert action.results.get("value") == "even more plain text2"

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


def test_unbalanced_versions_req_old_vs_prov_new(
    juju: JujuFixture,
):
    """Relating Requires (old version) with Provides (latest)."""
    downgrade_to_old_version(juju, APPLICATION_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.first_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Username
    leader_app_id = get_leader_id(juju, APPLICATION_APP_NAME)
    leader_app_name = f"{APPLICATION_APP_NAME}/{leader_app_id}"
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    action.wait()
    assert action.results.get("value")

    username = action.results.get("value")

    # Password
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value")

    password = action.results.get("value")

    # Username is correct
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    action.wait()
    assert action.results.get("value") == username

    # Password is correct
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password


def test_rolling_upgrade_requires_old_vs_provides_new(juju: JujuFixture):
    """Upgrading Requires to the latest version of the libs."""
    upgrade_to_new_version(juju, APPLICATION_APP_NAME)

    # Application charm leader
    leader_app_id = get_leader_id(juju, APPLICATION_APP_NAME)
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

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert not action.results.get("value")


def test_rolling_upgrade_requires_old_vs_provides_new_upgrade_new_secret(
    juju: JujuFixture,
):
    """After Requires upgrade new secrets are possible to define."""
    # Application charm leader
    leader_app_id = get_leader_id(juju, APPLICATION_APP_NAME)
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

    # Username exists
    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "username"},
    )
    action.wait()
    assert action.results.get("value")

    # Password exists
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.first_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value")


# -----------------------------------------------------
# Testing 'Requires - secrets' vs. 'Provides - databag'
# -----------------------------------------------------


def test_unbalanced_versions_req_new_vs_prov_old(
    juju: JujuFixture,
):
    """Relating Requires (latest) with Provides (old version)."""
    downgrade_to_old_version(juju, DATABASE_APP_NAME)

    # Storing Relation object in 'pytest' global namespace for the session
    pytest.second_database_relation = juju.ext.model.add_relation(
        f"{APPLICATION_APP_NAME}:{DB_SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.ext.model.wait_for_idle(apps=APP_NAMES, status="active")

    # Username exists
    leader_id = get_leader_id(juju, DATABASE_APP_NAME)
    leader_name = f"{DATABASE_APP_NAME}/{leader_id}"
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "username"},
    )
    action.wait()
    assert action.results.get("value")

    # Password exists
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value")


def test_rolling_upgrade_requires_new_vs_provides_old(
    juju: JujuFixture,
):
    """Upgrading Provides to latest version."""
    upgrade_to_new_version(juju, DATABASE_APP_NAME)

    # Application charm leader
    leader_app_id = get_leader_id(juju, APPLICATION_APP_NAME)
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

    # Interface functions (invoked by actions below) are consistent
    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "uris"},
    )
    action.wait()
    assert not action.results.get("value")


def test_rolling_upgrade_requires_new_vs_provides_old_upgrade_new_secret(
    juju: JujuFixture,
):
    """After Provider upgrade, we can safely define new secrets."""
    # Application charm leader
    leader_app_id = get_leader_id(juju, APPLICATION_APP_NAME)
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

    action = juju.ext.model.units.get(leader_app_name).run_action(
        "get-relation-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value")
    password = action.results.get("value")

    action = juju.ext.model.units.get(leader_name).run_action(
        "get-relation-self-side-field",
        **{"relation_id": pytest.second_database_relation.id, "field": "password"},
    )
    action.wait()
    assert action.results.get("value") == password
