import logging
import subprocess
from pathlib import Path
from literals import CA_CERT_PATH, CLIENT_CERT_PATH, CLIENT_KEY_PATH

logger = logging.getLogger(__name__)


def put(endpoints: str, key: str, value: str) -> str | None:
    """Put a key value pair in etcd."""
    if (
            not Path(CLIENT_CERT_PATH).exists()
            or not Path(CLIENT_KEY_PATH).exists()
            or not Path(CA_CERT_PATH).exists()
    ):
        logger.error("No client certificates available")
        return None
    try:
        output = subprocess.check_output(
            [
                "etcdctl",
                "--endpoints",
                endpoints,
                "--cert",
                CLIENT_CERT_PATH,
                "--key",
                CLIENT_KEY_PATH,
                "--cacert",
                CA_CERT_PATH,
                "put",
                key,
                value,
            ],
        )
    except subprocess.CalledProcessError:
        logger.error("etcdctl put failed")
        return None
    return output.decode("utf-8").strip()


def get(endpoints: str, key: str) -> str | None:
    """Get a key value pair from etcd."""
    if (
            not Path(CLIENT_CERT_PATH).exists()
            or not Path(CLIENT_KEY_PATH).exists()
            or not Path(CA_CERT_PATH).exists()
    ):
        logger.error("No client certificates available")
        return None
    try:
        output = subprocess.check_output(
            [
                "etcdctl",
                "--endpoints",
                endpoints,
                "--cert",
                CLIENT_CERT_PATH,
                "--key",
                CLIENT_KEY_PATH,
                "--cacert",
                CA_CERT_PATH,
                "get",
                key,
            ],
        )
    except subprocess.CalledProcessError:
        logger.error("etcdctl get failed")
        return None
    return output.decode("utf-8").strip()