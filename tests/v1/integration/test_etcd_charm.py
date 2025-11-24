#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

import pytest
from jubilant import Juju

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
REQUIRER_APP_NAME = "requirer-app"
TLS_NAME = "self-signed-certificates"
REQUIRER_TLS_NAME = "requirer-tls-provider"
PEER_RELATION = "etcd-peers"
INTERNAL_USER = "root"
APPS = [ETCD_APP_NAME, REQUIRER_APP_NAME, TLS_NAME, REQUIRER_TLS_NAME]
key_prefix = "/test/"


def get_requirer_common_name(juju: Juju) -> str:
    """Get the common name of the requirer charm."""
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_APP_NAME)))

    action = juju.run(unit=requirer_unit, action="get-credentials")
    if action.status == "completed":
        return action.results["username"]

    raise ValueError("Failed to get common name from requirer charm")


def get_requirer_mtls_certificate(juju: Juju) -> str | None:
    """Get the mtls certificate from the requirer TLS provider."""
    requirer_unit = next(iter(juju.status().get_units(REQUIRER_APP_NAME)))

    action = juju.run(unit=requirer_unit, action="get-certificate")
    if action.status == "completed":
        return action.results["certificate"]

    return None


@pytest.mark.abort_on_fail
async def test_deploy_charms(juju: Juju, application_charm, etcd_charm):
    """Deploy both charms (application and the testing charmed-etcd app) to use in the tests."""
    # Deploy both charms (1 unit for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    juju.deploy(application_charm, app=REQUIRER_APP_NAME, num_units=1)
    juju.deploy(ETCD_APP_NAME, channel="3.6/stable", num_units=3)
    juju.deploy(TLS_NAME, channel="1/edge", config={"ca-common-name": "etcd"})
    juju.deploy(
        TLS_NAME, app=REQUIRER_TLS_NAME, channel="1/edge", config={"ca-common-name": "etcd"}
    )

    # enable TLS and check if the cluster is still accessible
    logger.info("Integrating peer-certificates and client-certificates relations")
    juju.integrate(f"{ETCD_APP_NAME}:peer-certificates", TLS_NAME)
    juju.integrate(f"{ETCD_APP_NAME}:client-certificates", TLS_NAME)
    juju.integrate(REQUIRER_APP_NAME, REQUIRER_TLS_NAME)
    juju.wait(lambda status: apps_active_and_agents_idle(status, *APPS))


@pytest.mark.abort_on_fail
async def test_relate_client_charm(juju: Juju) -> None:
    """Test normal client charm relation."""
    juju.integrate(ETCD_APP_NAME, REQUIRER_APP_NAME)
    juju.wait(lambda status: apps_active_and_agents_idle(status, *APPS, idle_period=10))

    endpoints = get_cluster_endpoints(juju, ETCD_APP_NAME, tls_enabled=True)
    download_client_certificate_from_unit(juju)

    secret = get_secret_by_label_jubilant(juju, label=f"{PEER_RELATION}.{ETCD_APP_NAME}.app")
    assert secret, f"failed to get secret for {PEER_RELATION}.{ETCD_APP_NAME}.app"
    password = secret.get(f"{INTERNAL_USER}-password")

    # check if user and role are created for the common name and that the role is assigned to the user
    common_name = get_requirer_common_name(juju)
    logger.info(f"Requirer has common name: {common_name}")
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
        assert permission["key"] == key_prefix, "permission is not for the key prefix"

    # get client ca from every unit and check if it includes the mtls cert

    mtls_cert = get_requirer_mtls_certificate(juju)
    assert mtls_cert, "failed to get mtls cert from requirer TLS provider"
    for unit_name in juju.status().get_units(ETCD_APP_NAME):
        client_cas = get_certificate_from_unit(juju, unit_name, TLSType.CLIENT, is_ca=True)
        assert client_cas, f"failed to get client CAs for {unit_name}"
        assert mtls_cert in client_cas, f"mtls cert not in trusted CAs for {unit_name}"
