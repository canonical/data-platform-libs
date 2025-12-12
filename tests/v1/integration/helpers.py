#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import base64
import json
import logging
import subprocess
from datetime import datetime, timedelta
from enum import StrEnum
from time import sleep
from typing import Any, Dict, List, Optional

import jubilant
import yaml
from dateutil.parser import parse
from jubilant import Juju
from pytest_operator.plugin import OpsTest

PROV_SECRET_PREFIX = "secret-"
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


async def get_juju_secret(ops_test: OpsTest, secret_uri: str) -> Dict[str, str]:
    """Retrieve juju secret."""
    secret_unique_id = secret_uri.split("/")[-1]
    complete_command = f"show-secret {secret_uri} --reveal --format=json"
    _, stdout, _ = await ops_test.juju(*complete_command.split())
    return json.loads(stdout)[secret_unique_id]["content"]["Data"]


async def list_juju_secrets(ops_test: OpsTest) -> List[str]:
    """Check if a juju secret does not exist."""
    _, stdout, _ = await ops_test.juju("list-secrets")
    data = stdout.split("\n")
    data = data[1:]
    return [line.split()[0] for line in data if line]


async def get_leader_id(ops_test: OpsTest, app_name: str) -> int:
    """Returns the unit number of the juju leader unit."""
    for unit in ops_test.model.applications[app_name].units:
        if await unit.is_leader_from_status():
            return int(unit.name.split("/")[1])
    return -1


async def get_non_leader_id(ops_test: OpsTest, app_name: str) -> int:
    """Returns the unit number of the juju leader unit."""
    for unit in ops_test.model.applications[app_name].units:
        if not await unit.is_leader_from_status():
            return int(unit.name.split("/")[1])
    return -1


async def build_connection_string(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    *,
    relation_id: str | None = None,
    relation_alias: str | None = None,
) -> str:
    """Build a PostgreSQL connection string.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        relation_id: id of the relation to get connection data from
        relation_alias: alias of the relation (like a connection name)
            to get connection data from

    Returns:
        a PostgreSQL connection string
    """
    # Get the connection data exposed to the application through the relation.
    database = f"{application_name.replace('-', '_')}_{relation_name.replace('-', '_')}"

    requests = json.loads(
        await get_application_relation_data(
            ops_test, application_name, relation_name, "requests", relation_id, relation_alias
        )
        or "[]"
    )

    # We only have one request.
    request = requests[0]

    secret_uri = request[f"{PROV_SECRET_PREFIX}user"]
    secret_data = await get_juju_secret(ops_test, secret_uri)
    username = secret_data["username"]
    password = secret_data["password"]

    endpoints = request["endpoints"]
    host = endpoints.split(",")[0].split(":")[0]

    # Build the complete connection string to connect to the database.
    return f"dbname='{database}' user='{username}' host='{host}' password='{password}' connect_timeout=10"


async def get_connection_info(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    relation_id: str = None,
) -> str:
    """Build a dictionary that contains the connection info.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        relation_id: id of the relation to get connection data from

    Returns:
        a dictionary that contains connection info fields
    """
    # Get the connection data exposed to the application through the relation.

    access_key = await get_application_relation_data(
        ops_test, application_name, relation_name, "access-key", relation_id
    )
    secret_key = await get_application_relation_data(
        ops_test, application_name, relation_name, "secret-key", relation_id
    )
    endpoint = await get_application_relation_data(
        ops_test, application_name, relation_name, "endpoint", relation_id
    )
    bucket = await get_application_relation_data(
        ops_test, application_name, relation_name, "bucket", relation_id
    )
    path = await get_application_relation_data(
        ops_test, application_name, relation_name, "path", relation_id
    )
    region = await get_application_relation_data(
        ops_test, application_name, relation_name, "region", relation_id
    )
    s3_uri_style = await get_application_relation_data(
        ops_test, application_name, relation_name, "s3-uri-style", relation_id
    )
    storage_class = await get_application_relation_data(
        ops_test, application_name, relation_name, "storage-class", relation_id
    )
    tls_ca_chain = await get_application_relation_data(
        ops_test, application_name, relation_name, "tls-ca-chain", relation_id
    )
    s3_api_version = await get_application_relation_data(
        ops_test, application_name, relation_name, "s3-api-version", relation_id
    )
    attributes = await get_application_relation_data(
        ops_test, application_name, relation_name, "attributes", relation_id
    )

    connection_info = {
        "access-key": access_key,
        "secret-key": secret_key,
        "endpoint": endpoint,
        "bucket": bucket,
        "path": path,
        "region": region,
        "s3-uri-style": s3_uri_style,
        "storage-class": storage_class,
        "tls-ca-chain": tls_ca_chain,
        "s3-api-version": s3_api_version,
        "attributes": attributes,
    }
    # Return the connection info extracted from the relation.
    return connection_info


async def get_alias_from_relation_data(
    ops_test: OpsTest, unit_name: str, related_unit_name: str
) -> Optional[str]:
    """Get the alias that the unit assigned to the related unit application/cluster.

    Args:
        ops_test: The ops test framework instance
        unit_name: The name of the unit
        related_unit_name: name of the related unit

    Returns:
        the alias for the application/cluster of
            the related unit

    Raises:
        ValueError if it's not possible to get unit data
            or if there is no alias on that.
    """
    raw_data = (await ops_test.juju("show-unit", related_unit_name))[1]
    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {related_unit_name}")
    data = yaml.safe_load(raw_data)

    # Retrieve the relation data from the unit.
    relation_data = {}
    for relation in data[related_unit_name]["relation-info"]:
        # Peer relation typically
        if "related-units" not in relation:
            continue
        for name, unit in relation["related-units"].items():
            if name == unit_name:
                relation_data = unit["data"]
                break

    # Check whether the unit has set an alias for the related unit application/cluster.
    if "alias" not in relation_data:
        raise ValueError(f"no alias could be grabbed for {related_unit_name} application/cluster")

    return relation_data["alias"]


async def get_application_relation_data(
    ops_test: OpsTest,
    application_name: str,
    relation_name: str,
    key: str,
    relation_id: str | None = None,
    relation_alias: str | None = None,
    related_endpoint: str | None = None,
    app_or_unit: str = "app",
) -> Optional[str]:
    """Get relation data for an application.

    Args:
        ops_test: The ops test framework instance
        application_name: The name of the application
        relation_name: name of the relation to get connection data from
        key: key of data to be retrieved
        relation_id: id of the relation to get connection data from
        relation_alias: alias of the relation (like a connection name)
            to get connection data from
        related_endpoint: the related endpoint, i.e. the "other side" of the relation
        app_or_unit: whether it's the app or the unit databag to be searched

    Returns:
        the data that was requested or None
            if no data in the relation

    Raises:
        ValueError if it's not possible to get application data
            or if there is no data for the particular relation endpoint
            and/or alias.
    """
    unit_name = f"{application_name}/0"
    raw_data = (await ops_test.juju("show-unit", unit_name))[1]
    if not raw_data:
        raise ValueError(f"no unit info could be grabbed for {unit_name}")
    data = yaml.safe_load(raw_data)
    # Filter the data based on the relation name.
    relation_data = [v for v in data[unit_name]["relation-info"] if v["endpoint"] == relation_name]

    if relation_id:
        # Filter the data based on the relation id.
        relation_data = [v for v in relation_data if v["relation-id"] == relation_id]
    if relation_alias:
        # Filter the data based on the cluster/relation alias.
        relation_data = [
            v
            for v in relation_data
            if await get_alias_from_relation_data(
                ops_test, unit_name, next(iter(v["related-units"]))
            )
            == relation_alias
        ]
    if related_endpoint:
        relation_data = [
            v
            for v in data[unit_name]["relation-info"]
            if v["related-endpoint"] == related_endpoint
        ]
    if len(relation_data) == 0:
        raise ValueError(
            f"no relation data could be grabbed on relation with endpoint {relation_name} and alias {relation_alias}"
        )
    if app_or_unit == "app":
        return relation_data[0]["application-data"].get(key)
    else:
        return relation_data[0]["local-unit"].get("data", {}).get(key)


async def check_logs(ops_test: OpsTest, strings: list[str], limit: int = 10) -> bool:
    """Check if any of strings may appear in juju debug-log."""
    # juju debug-log may not be flushed yet, thus the "tenacity simulation"
    for tries in range(5):
        sleep(3)
        _, dbg_log, _ = await ops_test.juju("debug-log", "--no-tail", "--replay")
        if any(text in dbg_log for text in strings):
            return True
    return False


async def get_secret_by_label(ops_test, label: str, owner: str = "") -> Dict[str, str]:
    secrets_raw = await ops_test.juju("list-secrets")
    secret_ids = [
        secret_line.split()[0] for secret_line in secrets_raw[1].split("\n")[1:] if secret_line
    ]

    for secret_id in secret_ids:
        secret_data_raw = await ops_test.juju(
            "show-secret", "--format", "json", "--reveal", secret_id
        )
        secret_data = json.loads(secret_data_raw[1])

        if label == secret_data[secret_id].get("label"):
            if not owner or owner == secret_data[secret_id].get("owner"):
                return secret_data[secret_id]["content"]["Data"]


async def get_secret_revision_by_label(ops_test, label: str, owner: str = "") -> int:
    secrets_raw = await ops_test.juju("list-secrets")
    secret_ids = [
        secret_line.split()[0] for secret_line in secrets_raw[1].split("\n")[1:] if secret_line
    ]

    for secret_id in secret_ids:
        secret_data_raw = await ops_test.juju(
            "show-secret", "--format", "json", "--reveal", secret_id
        )
        secret_data = json.loads(secret_data_raw[1])

        if label == secret_data[secret_id].get("label"):
            if not owner or owner == secret_data[secret_id].get("owner"):
                return int(secret_data[secret_id]["revision"])


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
        etcd_command = f"{etcd_command} \
            --cacert client_ca.pem \
            --cert client.pem \
            --key client.key"

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
        etcd_command = f"{etcd_command} \
            --cacert client_ca.pem \
            --cert client.pem \
            --key client.key"
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
