import asyncio
import json
import logging
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .helpers import (
    get_application_relation_data,
)

logger = logging.getLogger(__name__)

APPLICATION_APP_1 = "appi"
APPLICATION_APP_2 = "appii"
DATABASE_APP_NAME = "database"
APP_NAMES = [APPLICATION_APP_1, APPLICATION_APP_2, DATABASE_APP_NAME]
DATABASE_APP_METADATA = yaml.safe_load(
    Path("./tests/v0/integration/database-charm/metadata.yaml").read_text()
)
RELATION_NAME = "first-database-db"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(
    ops_test: OpsTest,
    application_charm,
    database_charm,
    dp_libs_ubuntu_series,
):
    """Deploy both charms (application and database) to use in the tests."""
    await asyncio.gather(
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_1,
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
        ops_test.model.deploy(
            application_charm,
            application_name=APPLICATION_APP_2,
            num_units=1,
            series=dp_libs_ubuntu_series,
        ),
        ops_test.model.deploy(
            database_charm,
            resources={
                "database-image": DATABASE_APP_METADATA["resources"]["database-image"]["upstream-source"]  # fmt: skip
            },
            application_name=DATABASE_APP_NAME,
            num_units=1,
            series="jammy",
        ),
    )

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", idle_period=30)


@pytest.mark.abort_on_fail
async def test_relate_application(ops_test: OpsTest):
    # Relate the charms and wait for them exchanging some connection data.
    await ops_test.model.add_relation(DATABASE_APP_NAME, f"{APPLICATION_APP_1}:{RELATION_NAME}")
    await ops_test.model.add_relation(DATABASE_APP_NAME, f"{APPLICATION_APP_2}:{RELATION_NAME}")
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")


@pytest.mark.abort_on_fail
async def test_raise_status(ops_test: OpsTest):
    app_1_rel = next(iter(ops_test.model.applications[APPLICATION_APP_1].relations))
    app_2_rel = next(iter(ops_test.model.applications[APPLICATION_APP_2].relations))
    db_unit = ops_test.model.applications[DATABASE_APP_NAME].units[0].name

    # raise different status on different relations
    action = await ops_test.model.units.get(db_unit).run_action(
        "raise-status",
        **{"relation-id": app_1_rel.id, "status-code": 4001},
    )
    await action.wait()

    action = await ops_test.model.units.get(db_unit).run_action(
        "raise-status",
        **{"relation-id": app_2_rel.id, "status-code": 4002},
    )
    await action.wait()

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", idle_period=30)

    assert (
        "[4001]" in ops_test.model.applications[APPLICATION_APP_1].units[0].workload_status_message
    )
    assert (
        "[4002]" in ops_test.model.applications[APPLICATION_APP_2].units[0].workload_status_message
    )

    # raise another status on appi
    action = await ops_test.model.units.get(db_unit).run_action(
        "raise-status",
        **{"relation-id": app_1_rel.id, "status-code": 1000},
    )
    await action.wait()

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", idle_period=30)

    assert (
        "[1000, 4001]"
        in ops_test.model.applications[APPLICATION_APP_1].units[0].workload_status_message
    )
    assert (
        "[4002]" in ops_test.model.applications[APPLICATION_APP_2].units[0].workload_status_message
    )

    status_schema_raw = json.load(
        open("tests/v0/integration/database-charm/src/status-schema.json", "r")
    )
    status_schema_map = {o.get("code"): o for o in status_schema_raw.get("statuses", [])}

    # Verify rel data matches status schema
    rel_data_1 = await get_application_relation_data(
        ops_test, APPLICATION_APP_1, RELATION_NAME, "status"
    )
    assert rel_data_1
    assert len(json.loads(rel_data_1)) == 2
    for obj in json.loads(rel_data_1):
        original = status_schema_map.get(obj["code"], {})
        assert obj["message"] == original["message"]
        assert obj["resolution"] == original["resolution"]

    # TODO:


@pytest.mark.abort_on_fail
async def test_resolve_status(ops_test: OpsTest):
    app_1_rel = next(iter(ops_test.model.applications[APPLICATION_APP_1].relations))
    app_2_rel = next(iter(ops_test.model.applications[APPLICATION_APP_2].relations))
    db_unit = ops_test.model.applications[DATABASE_APP_NAME].units[0].name

    # resolve different status on different relations
    action = await ops_test.model.units.get(db_unit).run_action(
        "resolve-status",
        **{"relation-id": app_1_rel.id, "status-code": 4001},
    )
    await action.wait()

    action = await ops_test.model.units.get(db_unit).run_action(
        "resolve-status",
        **{"relation-id": app_2_rel.id, "status-code": 4002},
    )
    await action.wait()

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", idle_period=30)
    assert (
        "[1000]" in ops_test.model.applications[APPLICATION_APP_1].units[0].workload_status_message
    )
    assert "[]" in ops_test.model.applications[APPLICATION_APP_2].units[0].workload_status_message


@pytest.mark.abort_on_fail
async def test_clear_statuses(ops_test: OpsTest):
    app_1_rel = next(iter(ops_test.model.applications[APPLICATION_APP_1].relations))
    db_unit = ops_test.model.applications[DATABASE_APP_NAME].units[0].name

    # raise 4002 status on appi
    action = await ops_test.model.units.get(db_unit).run_action(
        "raise-status",
        **{"relation-id": app_1_rel.id, "status-code": 4002},
    )
    await action.wait()

    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", idle_period=30)
    assert (
        "[1000, 4002]"
        in ops_test.model.applications[APPLICATION_APP_1].units[0].workload_status_message
    )

    action = await ops_test.model.units.get(db_unit).run_action(
        "clear-statuses",
        **{"relation-id": app_1_rel.id},
    )

    # All statuses should be cleared
    await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active", idle_period=30)
    assert "[]" in ops_test.model.applications[APPLICATION_APP_1].units[0].workload_status_message
