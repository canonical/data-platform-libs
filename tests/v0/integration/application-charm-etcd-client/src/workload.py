import logging
import os
import platform
import shutil
import subprocess
import urllib
from pathlib import Path

from literals import CA_CERT_PATH, CLIENT_CERT_PATH, CLIENT_KEY_PATH, ETCD_VERSION

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


def install_etcdctl():
    """Install etcdctl using Python urllib and tar, accounting for architecture."""
    etcdctl_path = "/usr/local/bin/etcdctl"
    if shutil.which("etcdctl"):
        logger.info("etcdctl already installed.")
        return
    logger.info("Installing etcdctl via Python urllib...")
    arch = platform.machine()
    if arch == "aarch64":
        url = f"https://github.com/etcd-io/etcd/releases/download/v{ETCD_VERSION}/etcd-v{ETCD_VERSION}-linux-arm64.tar.gz"
    else:
        url = f"https://github.com/etcd-io/etcd/releases/download/v{ETCD_VERSION}/etcd-v{ETCD_VERSION}-linux-amd64.tar.gz"
    tmp_dir = "/tmp/etcd_install"
    os.makedirs(tmp_dir, exist_ok=True)
    tar_path = os.path.join(tmp_dir, "etcd.tar.gz")
    # Download tarball using urllib
    try:
        with urllib.request.urlopen(url) as response, open(tar_path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)
    except Exception as e:
        logger.error(f"Failed to download etcdctl tarball: {e}")
        return
    # Extract tarball
    try:
        subprocess.run(["tar", "-xvf", tar_path, "-C", tmp_dir], check=True)
    except Exception as e:
        logger.error(f"Failed to extract etcdctl tarball: {e}")
        return
    # Find the extracted etcdctl binary
    for entry in os.listdir(tmp_dir):
        if entry.startswith(f"etcd-v{ETCD_VERSION}-linux-"):
            etcdctl_src = os.path.join(tmp_dir, entry, "etcdctl")
            if os.path.isfile(etcdctl_src):
                try:
                    shutil.move(etcdctl_src, etcdctl_path)
                    os.chmod(etcdctl_path, 0o755)
                    logger.info(f"etcdctl installed at {etcdctl_path}")
                except Exception as e:
                    logger.error(f"Failed to move etcdctl binary: {e}")
                break
    else:
        logger.error("Failed to find etcdctl binary after extraction.")
