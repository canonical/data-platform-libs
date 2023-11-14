#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest
from pytest_operator.plugin import OpsTest


@pytest.fixture(scope="module")
def ops_test(ops_test: OpsTest) -> OpsTest:
    if os.environ.get("CI") == "true":
        # Running in GitHub Actions; skip build step
        # (GitHub Actions uses a separate, cached build step. See .github/workflows/ci.yaml)
        packed_charms = json.loads(os.environ["CI_PACKED_CHARMS"])

        async def build_charm(charm_path, bases_index: int = None) -> Path:
            for charm in packed_charms:
                if Path(charm_path) == Path(charm["directory_path"]):
                    if bases_index is None or bases_index == charm["bases_index"]:
                        return charm["file_path"]
            raise ValueError(f"Unable to find .charm file for {bases_index=} at {charm_path=}")

        ops_test.build_charm = build_charm
    return ops_test


@pytest.fixture(scope="module", autouse=True)
def copy_data_interfaces_library_into_charm(ops_test: OpsTest):
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
    install_path = "tests/integration/database-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/integration/kafka-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/integration/application-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/integration/opensearch-charm/" + library_path
    shutil.copyfile(library_path, install_path)


@pytest.fixture(scope="module", autouse=True)
def copy_s3_library_into_charm(ops_test: OpsTest):
    """Copy the s3 library to the applications charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/s3.py"
    install_path_provider = "tests/integration/s3-charm/" + library_path
    install_path_requirer = "tests/integration/application-s3-charm/" + library_path
    shutil.copyfile(library_path, install_path_provider)
    shutil.copyfile(library_path, install_path_requirer)


@pytest.fixture(scope="module")
async def application_charm(ops_test: OpsTest):
    """Build the application charm."""
    charm_path = "tests/integration/application-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def database_charm(ops_test: OpsTest):
    """Build the database charm."""
    charm_path = "tests/integration/database-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def application_s3_charm(ops_test: OpsTest):
    """Build the application-s3 charm."""
    charm_path = "tests/integration/application-s3-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def s3_charm(ops_test: OpsTest):
    """Build the S3 charm."""
    charm_path = "tests/integration/s3-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def kafka_charm(ops_test: OpsTest):
    """Build the Kafka charm."""
    charm_path = "tests/integration/kafka-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def opensearch_charm(ops_test: OpsTest):
    """Build the OpenSearch charm.

    TODO we could simplify a lot of these charm builds by having a single test charm that includes
    all these relations. This might be easily achieved by merging this repo with the
    data-integrator charm repo.
    """
    charm_path = "tests/integration/opensearch-charm"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(autouse=True)
async def without_errors(ops_test: OpsTest, request):
    """This fixture is to list all those errors that mustn't occur during execution."""
    # To be executed after the tests
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    yield
    whitelist = []
    if "log_errors_allowed" in request.keywords:
        for marker in [
            mark for mark in request.node.iter_markers() if mark.name == "log_errors_allowed"
        ]:
            for arg in marker.args:
                whitelist.append(arg)

        # All errors allowed
        if not whitelist:
            return

    _, dbg_log, _ = await ops_test.juju("debug-log", "--ms", "--replay")
    lines = dbg_log.split("\n")
    for index, line in enumerate(lines):
        logitems = line.split(" ")
        if not line or len(logitems) < 3:
            continue
        if logitems[1] < now:
            continue
        if logitems[2] == "ERROR":
            assert any(white in line for white in whitelist)
