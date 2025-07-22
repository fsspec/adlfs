import azure.storage.blob
import pytest
from azure.storage.blob.aio import BlobServiceClient as AIOBlobServiceClient

from adlfs import AzureBlobFile, AzureBlobFileSystem
from adlfs.tests.constants import CONN_STR, KEY
from adlfs.utils import __version__ as __version__


def assert_sets_adlfs_user_agent(mock_client_create_method):
    mock_client_create_method.assert_called()
    assert "user_agent" in mock_client_create_method.call_args.kwargs
    assert (
        mock_client_create_method.call_args.kwargs["user_agent"]
        == f"adlfs/{__version__}"
    )


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


def test_user_agent_blob_file_connection_str(
    storage: azure.storage.blob.BlobServiceClient, mock_from_connection_string
):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
    )
    f = AzureBlobFile(fs, "data/root/a/file.txt", mode="rb")
    f.connect_client()
    assert_sets_adlfs_user_agent(mock_from_connection_string)


def test_user_agent_blob_file_initializer(
    storage: azure.storage.blob.BlobServiceClient, mock_service_client_init
):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        account_key=KEY,
        skip_instance_cache=True,
    )
    f = AzureBlobFile(fs, "data/root/a/file.txt", mode="wb")
    f.connect_client()
    assert_sets_adlfs_user_agent(mock_service_client_init)
    # Makes sure no API calls are made that would cause the test to hang because of ssl connection issues
    f.closed = True


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
