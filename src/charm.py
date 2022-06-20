# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""A placeholder charm for the Data Platform libs."""

from ops.charm import CharmBase
from ops.main import main


class DataPlatformLibsCharm(CharmBase):
    """Placeholder charm for Data Platform libs."""


if __name__ == "__main__":
    main(DataPlatformLibsCharm)
