#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of global literals for the requirer charm for etcd interface testing (v0)."""

ETCD_DATA_DIR = "/var/lib/application-charm-etcd-client/etcd"
CLIENT_CERT_PATH = f"{ETCD_DATA_DIR}/client.pem"
CLIENT_KEY_PATH = f"{ETCD_DATA_DIR}/client.key"
CA_CERT_PATH = f"{ETCD_DATA_DIR}/ca.pem"
