import json
from unittest import mock

import pytest
from lib.charms.data_platform_libs.v0.azure_storage import (
    AzureStorageProvides,
    AzureStorageRequires,
    StorageConnectionInfoChangedEvent,
    StorageConnectionInfoGoneEvent,
    StorageConnectionInfoRequestedEvent,
)
from ops.charm import CharmBase
from ops.testing import Context, Relation, Secret, State

PROVIDER_APP = "azure-storage-provider"
REQUIRER_APP = "azure-storage-requirer"
RELATION_INTERFACE = "azure_storage"
RELATION_NAME = "azure-storage-credentials"

CONTAINER_NAME = "test-container"
SECRET_KEY = "asdfasdf"
STORAGE_ACCOUNT = "stoacc"
PATH = "testpath"
ENDPOINT = "http://localhost:80"
CONNECTION_PROTOCOL = "abfss"


class AzureStorageProviderCharm(CharmBase):
    """Mock the provider charm for Azure Storage relation for testing."""

    META = {
        "name": PROVIDER_APP,
        "provides": {RELATION_NAME: {"interface": RELATION_INTERFACE}},
    }

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = AzureStorageProvides(self, relation_name=RELATION_NAME)
        self.framework.observe(
            self.provider.on.storage_connection_info_requested, self._on_connection_info_requested
        )

    def _on_connection_info_requested(self, event: StorageConnectionInfoRequestedEvent) -> None:
        if not event.container:
            return

        self.provider.relation_data.update_relation_data(
            event.relation.id,
            {
                "container": CONTAINER_NAME,
                "connection-protocol": CONNECTION_PROTOCOL,
                "endpoint": ENDPOINT,
                "storage-account": STORAGE_ACCOUNT,
                "secret-key": SECRET_KEY,
                "path": PATH,
            },
        )


class AzureStorageRequirerCharm(CharmBase):

    META = {"name": REQUIRER_APP, "requires": {RELATION_NAME: {"interface": RELATION_INTERFACE}}}

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = AzureStorageRequires(
            self, relation_name=RELATION_NAME, container=CONTAINER_NAME
        )
        self.framework.observe(
            self.requirer.on.storage_connection_info_changed, self._on_connection_info_changed
        )
        self.framework.observe(
            self.requirer.on.storage_connection_info_gone, self._on_connection_info_gone
        )

    def _consume_azure_storage_credentials(self, connection_info: dict | None) -> None:
        if not connection_info:
            return
        print(f"Consuming Azure storage connection info: {connection_info}.")

    def _on_connection_info_changed(self, event: StorageConnectionInfoChangedEvent) -> None:
        relation = self.model.get_relation(RELATION_NAME)
        if not relation:
            return

        connection_info = self.requirer.fetch_relation_data(
            relation_ids=[
                relation.id,
            ]
        )[relation.id]
        self._consume_azure_storage_credentials(connection_info=connection_info)

    def _on_connection_info_gone(self, event: StorageConnectionInfoGoneEvent) -> None:
        self._consume_azure_storage_credentials(connection_info=None)


@pytest.mark.usefixtures("only_with_juju_secrets")
class TestAzureStorageProvider:

    def get_relation(self):
        return Relation(
            endpoint=RELATION_NAME,
            interface=RELATION_INTERFACE,
            remote_app_name=REQUIRER_APP,
            local_app_data={},
            remote_app_data={},
        )

    @property
    def context(self):
        return Context(
            charm_type=AzureStorageProviderCharm,
            meta=AzureStorageProviderCharm.META,
        )

    def test_connection_info_requested(self):
        relation = self.get_relation()
        state1 = State(relations=[relation], leader=True)
        relation.remote_app_data.update(
            {
                "container": CONTAINER_NAME,
                "requested-secrets": json.dumps(["secret-key"]),
            }
        )

        state2 = self.context.run(self.context.on.relation_changed(relation), state1)

        local_app_data = state2.get_relation(relation.id).local_app_data
        assert local_app_data["container"] == CONTAINER_NAME
        assert "secret-extra" in local_app_data

        assert local_app_data["storage-account"] == STORAGE_ACCOUNT
        assert local_app_data["endpoint"] == ENDPOINT
        assert local_app_data["connection-protocol"] == CONNECTION_PROTOCOL
        assert local_app_data["path"] == PATH

        secret_id = local_app_data["secret-extra"]
        secret_content = state2.get_secret(id=secret_id).latest_content
        assert secret_content is not None

        secret_key = secret_content["secret-key"]
        assert secret_key == SECRET_KEY

    def test_container_name_not_written_in_databag(self):
        relation = self.get_relation()
        state1 = State(relations=[relation], leader=True)
        relation.remote_app_data.update(
            {
                "requested-secrets": json.dumps(["secret-key"]),
            }
        )

        state2 = self.context.run(self.context.on.relation_changed(relation), state1)

        local_app_data = state2.get_relation(relation.id).local_app_data
        assert "container" not in local_app_data
        assert "endpoint" not in local_app_data
        assert "secret-extra" not in local_app_data
        assert "connection-protocol" not in local_app_data
        assert "storage-account" not in local_app_data


@pytest.mark.usefixtures("only_with_juju_secrets")
class TestAzureStorageRequirer:

    def get_relation(self) -> Relation:
        return Relation(
            endpoint=RELATION_NAME,
            interface=RELATION_INTERFACE,
            remote_app_name=PROVIDER_APP,
            local_app_data={
                "container": CONTAINER_NAME,
                "requested-secrets": json.dumps(["secret-key"]),
            },
            remote_app_data={},
        )

    @property
    def context(self):
        return Context(charm_type=AzureStorageRequirerCharm, meta=AzureStorageRequirerCharm.META)

    @mock.patch.object(AzureStorageRequirerCharm, "_consume_azure_storage_credentials")
    def test_storage_info_provided_incomplete_credentials(self, mock_consume):
        relation = self.get_relation()
        relation.remote_app_data.update(
            {
                "container": CONTAINER_NAME,
            }
        )
        state1 = State(
            relations=[relation],
            leader=True,
        )

        self.context.run(self.context.on.relation_changed(relation), state1)
        assert mock_consume.call_count == 0

    @mock.patch.object(AzureStorageRequirerCharm, "_consume_azure_storage_credentials")
    def test_storage_info_provided(self, mock_consume):
        relation = self.get_relation()
        credentials_secret = Secret(
            tracked_content={"secret-key": SECRET_KEY},
            label=f"{RELATION_NAME}.{relation.id}.extra.secret",
        )
        relation.remote_app_data.update(
            {
                "container": CONTAINER_NAME,
                "connection-protocol": CONNECTION_PROTOCOL,
                "endpoint": ENDPOINT,
                "storage-account": STORAGE_ACCOUNT,
                "path": PATH,
                "secret-extra": credentials_secret.id,
            }
        )
        state1 = State(relations=[relation], leader=True, secrets=[credentials_secret])

        self.context.run(self.context.on.relation_changed(relation), state1)
        args, kwargs = mock_consume.call_args
        connection_info = kwargs["connection_info"]

        assert connection_info["container"] == CONTAINER_NAME
        assert connection_info["connection-protocol"] == CONNECTION_PROTOCOL
        assert connection_info["endpoint"] == ENDPOINT
        assert connection_info["storage-account"] == STORAGE_ACCOUNT
        assert connection_info["secret-key"] == SECRET_KEY
        assert connection_info["path"] == PATH
