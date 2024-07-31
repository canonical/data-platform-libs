#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from subprocess import check_call, check_output

import pytest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def dp_libs_ubuntu_series(pytestconfig) -> str:
    if pytestconfig.option.os_series:
        return pytestconfig.option.os_series


@pytest.fixture(scope="module")
def ops_test(ops_test: OpsTest, pytestconfig) -> OpsTest:
    """Re-defining OpsTest.build_charm in a way that it takes CI caching and build parameters into account.

    Build parameters (for charms available for multiple OS versions) are considered both when building the
    charm, or when fetching pre-built, CI cached version of it.
    """
    _build_charm = ops_test.build_charm

    # Add bases_index option (indicating which OS version to use)
    # when building the charm within the scope of the test run
    async def build_charm(charm_path, bases_index: int = None) -> Path:
        if not bases_index and pytestconfig.option.build_bases_index is not None:
            bases_index = pytestconfig.option.build_bases_index

        logger.info(f"Building charm {charm_path} with base index {bases_index}")

        return await _build_charm(charm_path, bases_index=bases_index)

    ops_test.build_charm = build_charm
    return ops_test


@pytest.fixture(scope="module", autouse=True)
def copy_data_interfaces_library_into_charm(ops_test: OpsTest):
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
    install_path = "tests/integration/database-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/integration/dummy-database-charm/" + library_path
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
async def dummy_database_charm(ops_test: OpsTest):
    """Build the database charm."""
    charm_path = "tests/integration/dummy-database-charm"
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


@pytest.fixture(scope="module")
async def secrets_charm(ops_test: OpsTest):
    """Build the secrets charm."""
    charm_path = "tests/integration/secrets-charm"
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


@pytest.fixture(scope="session")
def fetch_old_versions():
    """Fetching the previous 4 versions of the lib for upgrade tests."""
    cwd = os.getcwd()
    src_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
    data_path = f"{cwd}/tests/integration/data/data_interfaces.py"
    tmp_path = "./tmp_repo_checkout"

    os.mkdir(tmp_path)
    os.chdir(tmp_path)
    check_call("git clone https://github.com/canonical/data-platform-libs.git", shell=True)
    os.chdir("data-platform-libs")
    last_commits = check_output(
        "git show --pretty=format:'%h' --no-patch -25", shell=True, universal_newlines=True
    ).split()

    versions = []
    for commit in last_commits:
        check_call(f"git checkout {commit}", shell=True)
        version = check_output(
            "grep ^LIBPATCH lib/charms/data_platform_libs/v0/data_interfaces.py | cut -d ' ' -f 3",
            shell=True,
            universal_newlines=True,
        )
        version = version.strip()
        if version not in versions:
            shutil.copyfile(src_path, f"{data_path}.v{version}")
            versions.append(version)

        if len(versions) == 7:
            break

    os.chdir(cwd)
    shutil.rmtree(tmp_path)
