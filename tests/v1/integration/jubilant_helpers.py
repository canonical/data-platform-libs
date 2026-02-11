#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
import base64
import json
import logging
import subprocess
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Dict

import jubilant
from dateutil.parser import parse
from jubilant import Juju

ETCD_APP_NAME = "charmed-etcd"
TLS_ROOT_DIR = "/var/snap/charmed-etcd/current/tls"
CLIENT_PORT = 2379

logger = logging.getLogger(__name__)


class SecretNotFoundError(Exception):
    """Raised when a secret is not found."""


class TLSType(StrEnum):
    """TLS types."""

    PEER = "peer"
    CLIENT = "client"


def apps_active_and_agents_idle(status: jubilant.Status, *apps: str, idle_period: int = 0) -> bool:
    """Check that all given apps are active, their agents idle (optional idle interval too) and optionally verify unit count as well.

    Args:
        status: represents the jubilant model's current status
        apps: A list of applications whose statuses to test against
        idle_period: Seconds to wait for the agents of each application unit to be idle.
    """
    return (
        jubilant.all_active(status, *apps)
        and jubilant.all_agents_idle(status, *apps)
        and check_apps_idle_period(status, *apps, idle_period=idle_period)
    )


def check_apps_idle_period(status: jubilant.Status, *apps: str, idle_period: int) -> bool:
    return all(
        parse(unit.juju_status.since, ignoretz=True) + timedelta(seconds=idle_period)
        < datetime.now()
        for app in apps
        for unit in status.get_units(app).values()
    )


def get_cluster_endpoints(
    juju: Juju, app_name: str = ETCD_APP_NAME, tls_enabled: bool = False
) -> str:
    """Resolve the etcd endpoints for a given juju application."""
    return ",".join(
        [
            f"{'https' if tls_enabled else 'http'}://{unit.public_address}:{CLIENT_PORT}"
            for unit in juju.status().get_units(app_name).values()
        ]
    )


def download_client_certificate_from_unit(juju: Juju, app_name: str = ETCD_APP_NAME) -> None:
    """Copy the client certificate files from a unit to the host's filesystem."""
    unit = next(iter(juju.status().get_units(app_name)))

    tls_path = TLS_ROOT_DIR

    for file in ["client.pem", "client.key", "client_ca.pem"]:
        juju.scp(f"{unit}:{tls_path}/{file}", file)


def get_secret_by_label_jubilant(juju: Juju, label: str) -> Dict[str, str]:
    for secret in juju.secrets():
        if label == secret.label:
            revealed_secret = juju.show_secret(secret.uri, reveal=True)
            return revealed_secret.content

    raise SecretNotFoundError(f"Secret with label {label} not found")


def get_user(
    endpoints: str,
    username: str,
    user: str | None = None,
    password: str | None = None,
    tls_enabled: bool = False,
) -> dict[str, Any] | None:
    """Get user details using `etcdctl`."""
    etcd_command = f"etcdctl user get {username} --endpoints={endpoints} -w json"
    if user:
        etcd_command = f"{etcd_command} --user={user}"
    if password:
        etcd_command = f"{etcd_command} --password={password}"
    if tls_enabled:
        etcd_command = f"{etcd_command} --cacert client_ca.pem --cert client.pem --key client.key"

    try:
        result = subprocess.getoutput(etcd_command)
        logger.debug(f"User get result: {result}")
        return json.loads(result)["roles"]
    except json.JSONDecodeError:
        return None


def get_role(
    endpoints: str,
    rolename: str,
    user: str | None = None,
    password: str | None = None,
    tls_enabled: bool = False,
) -> list[dict[str, str]] | None:
    """Get role details using `etcdctl`."""
    etcd_command = f"etcdctl role get {rolename} --endpoints={endpoints} -w json"
    if user:
        etcd_command = f"{etcd_command} --user={user}"
    if password:
        etcd_command = f"{etcd_command} --password={password}"
    if tls_enabled:
        etcd_command = f"{etcd_command} --cacert client_ca.pem --cert client.pem --key client.key"
    try:
        result = json.loads(subprocess.getoutput(etcd_command))["perm"]
        return [
            {
                "permType": perm["permType"],
                "key": base64.b64decode(perm["key"]).decode("utf-8"),
                "range_end": base64.b64decode(perm["range_end"]).decode("utf-8"),
            }
            for perm in result
        ]
    except json.JSONDecodeError:
        return None


def get_certificate_from_unit(
    juju: Juju, unit: str, cert_type: TLSType, is_ca: bool = False
) -> str | None:
    """Retrieve a certificate from a unit."""
    command = f"cat {TLS_ROOT_DIR}/{cert_type.value}{'_ca' if is_ca else ''}.pem"
    output = juju.ssh(target=unit, command=command)
    if output.startswith("-----BEGIN CERTIFICATE-----"):
        return output

    return None
