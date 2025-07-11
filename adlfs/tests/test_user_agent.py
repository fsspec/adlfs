import datetime

import azure.storage.blob
import pytest
from azure.storage.blob import BlobServiceClient, PublicAccess
from azure.storage.blob.aio import BlobServiceClient as AIOBlobServiceClient

from adlfs import AzureBlobFile, AzureBlobFileSystem
from adlfs.utils import __version__ as __version__

URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA
DEFAULT_VERSION_ID = "1970-01-01T00:00:00.0000000Z"
LATEST_VERSION_ID = "2022-01-01T00:00:00.0000000Z"

_USER_AGENT = f"adlfs/{__version__}"


def assert_sets_adlfs_user_agent(mock_client):
    mock_client.assert_called_once()
    assert "user_agent" in mock_client.call_args.kwargs
    assert mock_client.call_args.kwargs["user_agent"] == _USER_AGENT


@pytest.fixture()
def mock_from_connection_string(mocker):
    return mocker.patch.object(
        AIOBlobServiceClient,
        "from_connection_string",
        autospec=True,
        side_effect=AIOBlobServiceClient.from_connection_string,
    )


@pytest.fixture()
def mock_service_client_init(mocker):
    return mocker.patch.object(
        AIOBlobServiceClient,
        "__init__",
        autospec=True,
        side_effect=AIOBlobServiceClient.__init__,
    )


@pytest.fixture(scope="function")
def mock_container(host):
    conn_str = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA

    bbs = BlobServiceClient.from_connection_string(conn_str=conn_str)
    if "data2" not in [c["name"] for c in bbs.list_containers()]:
        bbs.create_container("data2", public_access=PublicAccess.Container)
    container_client = bbs.get_container_client(container="data2")
    bbs.insert_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(
        microsecond=0
    )
    container_client.upload_blob("root/a/file.txt", b"0123456789")

    yield bbs

    bbs.delete_container("data2")


def test_user_agent_blob_file_connection_str(
    storage: azure.storage.blob.BlobServiceClient, mock_from_connection_string
):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
    )
    AzureBlobFile(fs, "data/root/a/file.txt", mode="rb")

    assert_sets_adlfs_user_agent(mock_from_connection_string)


def test_user_agent_blob_file_initializer(mock_container, mock_service_client_init):
    fs = AzureBlobFileSystem(
        account_name=ACCOUNT_NAME,
        account_host="http://127.0.0.1:10000/devstoreaccount1",
        account_key=KEY,
        skip_instance_cache=True,
    )
    AzureBlobFile(fs, "data2/root/a/file.txt", mode="rb")
    assert_sets_adlfs_user_agent(mock_service_client_init)


def test_user_agent_connection_str(
    storage: azure.storage.blob.BlobServiceClient, mock_from_connection_string
):
    AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
    )
    assert_sets_adlfs_user_agent(mock_from_connection_string)


def test_user_agent_initializer(
    storage: azure.storage.blob.BlobServiceClient, mock_service_client_init
):
    AzureBlobFileSystem(
        account_name=storage.account_name,
        skip_instance_cache=True,
    )
    assert_sets_adlfs_user_agent(mock_service_client_init)
