import pytest
from charms.data_platform_libs.v0.upgrade import (
    build_complete_sem_ver,
    verify_caret_requirements,
    verify_tilde_requirements,
)


@pytest.mark.parametrize(
    "version,output",
    [
        ("0.0.24.0.4", [0, 0, 24]),
        ("3.5.3", [3, 5, 3]),
        ("0.3", [0, 3, 0]),
        ("1.2", [1, 2, 0]),
        ("3.5.*", [3, 5, 0]),
        ("0.*", [0, 0, 0]),
        ("1.*", [1, 0, 0]),
        ("1.2.*", [1, 2, 0]),
        ("*", [0, 0, 0]),
        (1, [1, 0, 0]),
    ],
)
def test_build_complete_sem_ver(version, output):
    assert build_complete_sem_ver(version) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("~1.2.3", "1.2.2", True),
        ("1.2.3", "1.2.2", True),
        ("^1.2.3", "1.2.2", False),
        ("^1.2.3", "1.3", True),
        ("^1.2.3", "2.2.5", False),
        ("^1.2", "1.2.2", True),
        ("^1.2.3", "1.2", False),
        ("^1.2.3", "2.2.5", False),
        ("^1", "1.2.2", True),
        ("^1", "1.6", True),
        ("^1", "1.7.9", True),
        ("^1", "0.6", False),
        ("^1", "2", False),
        ("^1", "2.3", False),
        ("^0.2.3", "0.2.2", False),
        ("^0.2.3", "0.2.5", True),
        ("^0.2.3", "1.2.5", False),
        ("^0.2.3", "0.3.6", False),
        ("^0.0.3", "0.0.4", False),
        ("^0.0.3", "0.0.2", False),
        ("^0.0.3", "0.0", False),
        ("^0.0.3", "0.3.6", False),
        ("^0.0", "0.0.3", True),
        ("^0.0", "0.1.0", False),
        ("^0", "0.1.0", True),
        ("^0", "0.3.6", True),
        ("^0", "1.0.0", False),
    ],
)
def test_verify_caret_requirements(requirement, version, output):
    assert verify_caret_requirements(version=version, requirement=requirement) == output


@pytest.mark.parametrize(
    "requirement,version,output",
    [
        ("^1.2.3", "1.2.2", True),
        ("1.2.3", "1.2.2", True),
        ("~1.2.3", "1.2.2", False),
        ("~1.2.3", "1.3.2", False),
        ("~1.2.3", "1.3.5", False),
        ("~1.2.3", "1.2.5", True),
        ("~1.2.3", "1.2", False),
        ("~1.2", "1.2", True),
        ("~1.2", "1.6", False),
        ("~1.2", "1.2.4", True),
        ("~1.2", "1.1", False),
        ("~1.2", "1.0.5", False),
        ("~0.2", "0.2", True),
        ("~0.2", "0.2.3", True),
        ("~0.2", "0.3", False),
        ("~1", "0.3", False),
        ("~1", "1.3", True),
        ("~1", "0.0.9", False),
        ("~1", "0.9.9", False),
        ("~1", "1.9.9", True),
        ("~1", "1.7", True),
        ("~1", "1", True),
        ("~0", "1", False),
        ("~0", "0.1", True),
        ("~0", "0.5.9", True),
    ],
)
def test_verify_tilde_requirements(requirement, version, output):
    assert verify_tilde_requirements(version=version, requirement=requirement) == output
