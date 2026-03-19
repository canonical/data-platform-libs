#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os
import shutil
from datetime import datetime
from subprocess import check_call, check_output

import pytest
from jubilant_adapters import JujuFixture, temp_model_fixture

logger = logging.getLogger(__name__)

USE_CACHED_BUILD = bool(os.environ.get("CI", False))


@pytest.fixture(scope="session")
def dp_libs_ubuntu_series(pytestconfig) -> str:
    if pytestconfig.option.os_series:
        return pytestconfig.option.os_series


def pytest_addoption(parser):
    """Defines pytest parsers."""
    parser.addoption(
        "--model",
        action="store",
        help="Juju model to use; if not provided, a new model "
        "will be created for each test which requires one",
    )
    parser.addoption(
        "--keep-models",
        action="store_true",
        help="Keep models handled by opstest, can be overridden in track_model",
    )


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Pytest fixture that wraps :meth:`jubilant.with_model`.

    This adds command line parameter ``--keep-models`` (see help for details).
    """
    model = request.config.getoption("--model")
    keep_models = bool(request.config.getoption("--keep-models"))

    if model:
        juju = JujuFixture(model=model)
        yield juju
    else:
        with temp_model_fixture(keep=keep_models) as juju:
            yield juju


@pytest.fixture(scope="module", autouse=True)
def copy_data_interfaces_library_into_charm(juju: JujuFixture):
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
    install_path = "tests/v0/integration/database-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/v0/integration/dummy-database-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/v0/integration/kafka-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/v0/integration/application-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/v0/integration/opensearch-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/v0/integration/kafka-connect-charm/" + library_path
    shutil.copyfile(library_path, install_path)
    install_path = "tests/v0/integration/application-charm-etcd-client/" + library_path
    shutil.copyfile(library_path, install_path)


@pytest.fixture(scope="module", autouse=True)
def copy_s3_library_into_charm(juju: JujuFixture):
    """Copy the s3 library to the applications charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/s3.py"
    install_path_provider = "tests/v0/integration/s3-charm/" + library_path
    install_path_requirer = "tests/v0/integration/application-s3-charm/" + library_path
    shutil.copyfile(library_path, install_path_provider)
    shutil.copyfile(library_path, install_path_requirer)


@pytest.fixture(scope="module")
def application_charm(juju: JujuFixture):
    """Build the application charm."""
    charm_path = "tests/v0/integration/application-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def application_charm_etcd_client(juju: JujuFixture):
    """Build the application charm."""
    charm_path = "tests/v0/integration/application-charm-etcd-client"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def database_charm(juju: JujuFixture):
    """Build the database charm."""
    charm_path = "tests/v0/integration/database-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def dummy_database_charm(juju: JujuFixture):
    """Build the database charm."""
    charm_path = "tests/v0/integration/dummy-database-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def application_s3_charm(juju: JujuFixture):
    """Build the application-s3 charm."""
    charm_path = "tests/v0/integration/application-s3-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def s3_charm(juju: JujuFixture):
    """Build the S3 charm."""
    charm_path = "tests/v0/integration/s3-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def kafka_charm(juju: JujuFixture):
    """Build the Kafka charm."""
    charm_path = "tests/v0/integration/kafka-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def kafka_connect_charm(juju: JujuFixture):
    """Build the Kafka Connect dummy charm."""
    charm_path = "tests/v0/integration/kafka-connect-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def opensearch_charm(juju: JujuFixture):
    """Build the OpenSearch charm.

    TODO we could simplify a lot of these charm builds by having a single test charm that includes
    all these relations. This might be easily achieved by merging this repo with the
    data-integrator charm repo.
    """
    charm_path = "tests/v0/integration/opensearch-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(scope="module")
def secrets_charm(juju: JujuFixture):
    """Build the secrets charm."""
    charm_path = "tests/v0/integration/secrets-charm"
    charm = juju.ext.build_charm(charm_path, use_cache=USE_CACHED_BUILD)
    return charm


@pytest.fixture(autouse=True)
def without_errors(juju: JujuFixture, request):
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

    _, dbg_log, _ = juju.juju("debug-log", "--ms", "--replay")
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
    data_path = f"{cwd}/tests/v0/integration/data/data_interfaces.py"
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
