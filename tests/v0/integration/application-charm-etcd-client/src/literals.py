#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of global literals for the requirer charm for etcd interface testing (v0)."""

SNAP_DIR = "/var/snap/charmed-etcd/common"
CLIENT_CERT_PATH = f"{SNAP_DIR}/client.pem"
CLIENT_KEY_PATH = f"{SNAP_DIR}/client.key"
CA_CERT_PATH = f"{SNAP_DIR}/ca.pem"
SNAP_NAME = "charmed-etcd"
