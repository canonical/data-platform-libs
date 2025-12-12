#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
from datetime import timedelta

import pytest
from charmlibs.interfaces.tls_certificates import (
    Certificate,
    CertificateRequestAttributes,
    PrivateKey,
    generate_csr,
)
from jubilant import Juju, TaskError

from tests.v1.integration.helpers import (
    TLSType,
    apps_active_and_agents_idle,
    download_client_certificate_from_unit,
    get_certificate_from_unit,
    get_cluster_endpoints,
    get_role,
    get_secret_by_label_jubilant,
    get_user,
)

logger = logging.getLogger(__name__)


ETCD_APP_NAME = "charmed-etcd"
ETCD_APP_CLIENT_ENDPOINT = "etcd-client"
REQUIRER_APP_NAME = "requirer-app"
REQUIRER_ETCD_CLIENT_ENDPOINT = "etcd-client"
TLS_NAME = "self-signed-certificates"
PEER_RELATION = "etcd-peers"
INTERNAL_USER = "root"
APPS = [ETCD_APP_NAME, REQUIRER_APP_NAME, TLS_NAME]
key_prefix = "/test/"
TEST_KEY = "test_key"
TEST_VALUE = "42"


def get_requirer_common_names(juju: Juju) -> list[str]:
    """Get the common name of the requirer charm."""
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_APP_NAME)))

    action_result = juju.run(requirer_unit, "get-credentials")
    if action_result.status == "completed":
        return action_result.results["username"].split(",")

    raise ValueError("Failed to get common name from requirer charm")


def get_requirer_mtls_certificates(juju: Juju) -> list[str] | None:
    """Get the mtls certificate from the requirer TLS provider."""
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_APP_NAME)))

    action_result = juju.run(requirer_unit, "get-certificates")
    if action_result.status == "completed":
        return json.loads(action_result.results["certificates"])

    return None


def generate_mtls_chain(common_name: str) -> tuple[str, str]:
    """Generate a mtls certificate chain with a CA and an end-entity certificate.

    Args:
        common_name (str): The common name for the end-entity certificate.

    Returns:
        tuple[str, str]: The end-entity certificate and the CA certificate.
    """
    ca_private_key = PrivateKey.generate()
    ca_cert = Certificate.generate_self_signed_ca(
        private_key=ca_private_key,
        validity=timedelta(days=365),
        attributes=CertificateRequestAttributes(common_name="ca_common_name"),
    )

    client_private_key = PrivateKey.generate()
    client_csr = generate_csr(private_key=client_private_key, common_name=common_name)
    client_cert = Certificate.generate(
        csr=client_csr, ca=ca_cert, ca_private_key=ca_private_key, validity=timedelta(days=365)
    )
    return client_cert.raw, ca_cert.raw


@pytest.mark.abort_on_fail
def test_deploy_charms(etcd_charm: str, juju_lxd_model: Juju, application_charm):
    """Deploy both charms (application and the testing charmed-etcd app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    juju_lxd_model.deploy(application_charm, app=REQUIRER_APP_NAME, num_units=1)
    juju_lxd_model.deploy(etcd_charm, app=ETCD_APP_NAME, num_units=3)
    juju_lxd_model.deploy(TLS_NAME, channel="1/edge", config={"ca-common-name": "etcd"})

    # enable TLS and check if the cluster is still accessible
    logger.info("Integrating peer-certificates and client-certificates relations")
    juju_lxd_model.integrate(f"{ETCD_APP_NAME}:peer-certificates", TLS_NAME)
    juju_lxd_model.integrate(f"{ETCD_APP_NAME}:client-certificates", TLS_NAME)
    juju_lxd_model.integrate(REQUIRER_APP_NAME, TLS_NAME)
    juju_lxd_model.wait(
        lambda status: apps_active_and_agents_idle(
            status, ETCD_APP_NAME, TLS_NAME, REQUIRER_APP_NAME, idle_period=10
        ),
        timeout=1200,
        successes=1,
    )


@pytest.mark.abort_on_fail
def test_relate_client_charm(
    juju_lxd_model: Juju,
    lxd_controller: str,
) -> None:
    """Test normal client charm relation."""
    juju_lxd_model.integrate(ETCD_APP_NAME, REQUIRER_APP_NAME)
    juju_lxd_model.wait(
        lambda status: apps_active_and_agents_idle(
            status, ETCD_APP_NAME, REQUIRER_APP_NAME, idle_period=10
        )
    )

    endpoints = get_cluster_endpoints(juju_lxd_model, ETCD_APP_NAME, tls_enabled=True)
    download_client_certificate_from_unit(juju_lxd_model, ETCD_APP_NAME)
    secret = get_secret_by_label_jubilant(
        juju_lxd_model, label=f"{PEER_RELATION}.{ETCD_APP_NAME}.app"
    )
    assert secret, f"failed to get secret for {PEER_RELATION}.{ETCD_APP_NAME}.app"
    password = secret.get(f"{INTERNAL_USER}-password")

    # check if user and role are created for the common name and that the role is assigned to the user
    common_names = get_requirer_common_names(juju_lxd_model)
    logger.info(f"Requirer has common names: {common_names}")
    for common_name in common_names:
        user_roles = get_user(
            endpoints, common_name, user=INTERNAL_USER, password=password, tls_enabled=True
        )
        assert user_roles, f"failed to get user roles for {common_name}"
        assert common_name in user_roles, f"failed to get user roles for {common_name}"

        # check if the user can read and write to the key prefix
        permissions = get_role(
            endpoints, common_name, user=INTERNAL_USER, password=password, tls_enabled=True
        )

        assert permissions, f"failed to get permissions for {common_name}"
        for permission in permissions:
            assert permission["permType"] == 2, "permission is not read and write"
            assert permission["key"] == f"/{common_name}/", "permission is not for the key prefix"

    # get client ca from every unit and check if it includes the mtls cert
    mtls_certs = get_requirer_mtls_certificates(juju_lxd_model)
    assert mtls_certs, "failed to get mtls cert from requirer TLS provider"
    for unit_name in juju_lxd_model.status().get_units(ETCD_APP_NAME):
        client_cas = get_certificate_from_unit(
            juju_lxd_model, unit_name, TLSType.CLIENT, is_ca=True
        )
        assert client_cas, f"failed to get client CAs for {unit_name}"
        for mtls_cert in mtls_certs:
            assert mtls_cert in client_cas, f"mtls cert not in trusted CAs for {unit_name}"


@pytest.mark.abort_on_fail
async def test_write_read_with_requirer(juju_lxd_model: Juju) -> None:
    """Test write and read to the key prefix with the requirer charm."""
    requirer_unit = next(iter(juju_lxd_model.status().get_units(REQUIRER_APP_NAME)))

    # write to the key prefix
    with pytest.raises(TaskError) as task_error:
        juju_lxd_model.run(
            requirer_unit, "put-etcd", params={"key": TEST_KEY, "value": TEST_VALUE}
        )
    assert "permission denied" in str(
        task_error
    ), "Action should fail because user does not have permission to write to the key prefix"

    # write to authorized key prefix
    # every user will write the key to their own prefix
    key = "/test/foo"
    action = juju_lxd_model.run(
        requirer_unit, "put-etcd", params={"key": key, "value": TEST_VALUE}
    )
    assert action.status == "completed", "Action should succeed"

    # read from the key prefix
    action = juju_lxd_model.run(requirer_unit, "get-etcd", params={"key": key})
    assert action.status == "completed", "Action should succeed"
    common_names = get_requirer_common_names(juju_lxd_model)
    results = json.loads(action.results["results"])
    for common_name in common_names:
        assert (
            common_name in results
            and results[common_name] == f"/{common_name}/{key}\n{TEST_VALUE}"
        )


@pytest.mark.abort_on_fail
def test_update_mtls_cert(juju_lxd_model: Juju) -> None:
    """Test updating the common name used by the requirer app."""
    old_mtls_certs = get_requirer_mtls_certificates(juju_lxd_model)
    assert old_mtls_certs, "failed to get the old mtls certs from requirer TLS provider"

    # run juju action to update the common name
    requirer_unit = next(iter(juju_lxd_model.status().get_units(REQUIRER_APP_NAME)))

    juju_lxd_model.run(requirer_unit, "update-mtls-certs")

    # wait for model to settle
    juju_lxd_model.wait(
        lambda status: apps_active_and_agents_idle(
            status, ETCD_APP_NAME, REQUIRER_APP_NAME, idle_period=10
        )
    )

    # get client ca from every unit and check if it includes the new_ca
    mtls_certs = get_requirer_mtls_certificates(juju_lxd_model)
    assert mtls_certs, "failed to get the new mtls certs from requirer TLS provider"

    for unit_name in juju_lxd_model.status().get_units(ETCD_APP_NAME):
        client_cas = get_certificate_from_unit(
            juju_lxd_model, unit_name, TLSType.CLIENT, is_ca=True
        )
        assert client_cas, f"failed to get client CAs for {unit_name}"
        for mtls_cert in mtls_certs:
            assert mtls_cert in client_cas, f"new mtls cert not in trusted CAs for {unit_name}"
        for old_mtls_cert in old_mtls_certs:
            assert (
                old_mtls_cert not in client_cas
            ), f"old mtls certificate still in trusted CAs for {unit_name}"
