# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""Library to provide hardware architecture checks for VMs and K8s charms.

The WrongArchitectureWarningCharm class is designed to be used alongside
the is-wrong-architecture helper function, as follows:

```python
import sys

from ops import main
from charms.data_platform_libs.v0.architecture import (
    WrongArchitectureWarningCharm,
    is_wrong_architecture,
)

if __name__ == "__main__":
    if is_wrong_architecture():
        main(WrongArchitectureWarningCharm)
        sys.exit(1)
```

"""

import logging
import os
import sys

from ops import BlockedStatus, CharmBase

# The unique Charmhub library identifier, never change it
LIBID = "e08effb53f984b29b8e3b3094787d1b7"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

PYDEPS = ["ops>=2.0.0"]


logger = logging.getLogger(__name__)


class WrongArchitectureWarningCharm(CharmBase):
    """A fake charm class that only signals a wrong architecture deploy."""

    def __init__(self, *args):
        super().__init__(*args)

        hw_arch = os.uname().machine
        self.unit.status = BlockedStatus(f"Error: Charm incompatible with {hw_arch} architecture")
        sys.exit(0)


def is_wrong_architecture() -> bool:
    """Checks if charm was deployed on wrong architecture."""
    manifest_path = f"{os.environ.get('CHARM_DIR')}/manifest.yaml"

    if not os.path.exists(manifest_path):
        logger.error("Cannot check architecture: manifest file not found in %s", manifest_path)
        return False

    with open(manifest_path, "r") as file:
        manifest = file.read()

    hw_arch = os.uname().machine
    if ("amd64" in manifest and hw_arch == "x86_64") or (
        "arm64" in manifest and hw_arch == "aarch64"
    ):
        logger.info("Charm architecture matches")
        return False

    logger.error("Charm architecture does not match")
    return True
