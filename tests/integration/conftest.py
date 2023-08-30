#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import os
import shutil
from pathlib import Path

import pytest
from pytest_operator.plugin import OpsTest

LIB_VERSION = os.environ.get("LIB_VERSION")

CHARM_DB_PATH = "tests/integration/charms/database-charm/"
CHARM_APP_PATH = "tests/integration/charms/application-charm/"
CHARM_KAFKA_PATH = "tests/integration/charms/kafka-charm/"
CHARM_OPENSEARCH_PATH = "tests/integration/charms/opensearch-charm/"

CHARM_S3_PROVIDES_PATH = "tests/integration/charms/s3-charm/"
CHARM_S3_REQUIRES_PATH = "tests/integration/charms/application-s3-charm/"

CHARMS_PATH = [CHARM_DB_PATH, CHARM_APP_PATH, CHARM_KAFKA_PATH, CHARM_OPENSEARCH_PATH]
CHARMS_S3_PATH = [CHARM_S3_PROVIDES_PATH, CHARM_S3_REQUIRES_PATH]


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
def cleanup_charms_libs(ops_test: OpsTest):
    """Cleaning up charms libs.

    IMPORTANT to make sure we're running the correct major library version, in a multi-versions space.
    """
    for path in CHARMS_PATH + CHARMS_S3_PATH:
        install_file = path + "lib"
        shutil.rmtree(install_file, ignore_errors=True)

    yield

    for path in CHARMS_PATH + CHARMS_S3_PATH:
        install_file = path + "lib"
        shutil.rmtree(install_file, ignore_errors=True)


@pytest.fixture(scope="module", autouse=True)
def copy_data_interfaces_library_into_charm(ops_test: OpsTest, cleanup_charms_libs):
    """Copy the data_interfaces library to the different charm folder, clean up after."""
    library_path = f"lib/charms/data_platform_libs/{LIB_VERSION}/data_interfaces.py"

    for path in CHARMS_PATH:
        install_file = path + library_path
        os.makedirs(os.path.dirname(install_file), exist_ok=True)
        shutil.copyfile(library_path, install_file)


@pytest.fixture(scope="module", autouse=True)
def copy_s3_library_into_charm(ops_test: OpsTest, cleanup_charms_libs):
    """Copy the s3 library to the applications charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/s3.py"
    install_path_provider = CHARM_S3_PROVIDES_PATH + library_path
    install_path_requirer = CHARM_S3_REQUIRES_PATH + library_path

    for dst_lib_path in [install_path_requirer, install_path_provider]:
        os.makedirs(os.path.dirname(dst_lib_path), exist_ok=True)
        shutil.copyfile(library_path, dst_lib_path)


@pytest.fixture(scope="module")
async def application_charm(ops_test: OpsTest):
    """Build the application charm."""
    charm_path = CHARM_APP_PATH
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def database_charm(ops_test: OpsTest):
    """Build the database charm."""
    charm_path = CHARM_DB_PATH
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def application_s3_charm(ops_test: OpsTest):
    """Build the application-s3 charm."""
    charm_path = CHARM_S3_REQUIRES_PATH
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def s3_charm(ops_test: OpsTest):
    """Build the S3 charm."""
    charm_path = CHARM_S3_PROVIDES_PATH
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def kafka_charm(ops_test: OpsTest):
    """Build the Kafka charm."""
    charm_path = CHARM_KAFKA_PATH
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def opensearch_charm(ops_test: OpsTest):
    """Build the OpenSearch charm.

    TODO we could simplify a lot of these charm builds by having a single test charm that includes
    all these relations. This might be easily achieved by merging this repo with the
    data-integrator charm repo.
    """
    charm_path = CHARM_OPENSEARCH_PATH
    charm = await ops_test.build_charm(charm_path)
    return charm
