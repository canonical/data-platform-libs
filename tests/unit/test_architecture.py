#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import unittest.mock as mock
from unittest.mock import patch

from charms.data_platform_libs.v0.architecture import is_wrong_architecture


def test_wrong_architecture_file_not_found():
    """Tests if the function returns False when the charm file doesn't exist."""
    with (
        patch("os.environ.get", return_value="/tmp"),
        patch("os.path.exists", return_value=False),
    ):
        assert not is_wrong_architecture()


def test_wrong_architecture_amd64():
    """Tests if the function correctly identifies arch when charm is AMD."""
    with (
        patch("os.environ.get", return_value="/tmp"),
        patch("os.path.exists", return_value=True),
        patch("builtins.open", mock.mock_open(read_data="amd64\n")),
        patch("os.uname") as _uname,
    ):
        _uname.return_value = mock.Mock(machine="x86_64")
        assert not is_wrong_architecture()
        _uname.return_value = mock.Mock(machine="aarch64")
        assert is_wrong_architecture()


def test_wrong_architecture_arm64():
    """Tests if the function correctly identifies arch when charm is ARM."""
    with (
        patch("os.environ.get", return_value="/tmp"),
        patch("os.path.exists", return_value=True),
        patch("builtins.open", mock.mock_open(read_data="arm64\n")),
        patch("os.uname") as _uname,
    ):
        _uname.return_value = mock.Mock(machine="x86_64")
        assert is_wrong_architecture()
        _uname.return_value = mock.Mock(machine="aarch64")
        assert not is_wrong_architecture()
