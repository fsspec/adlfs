from unittest import mock

import azure.storage.blob
import pytest
from azure.storage.blob.aio import BlobServiceClient as AIOBlobServiceClient

from adlfs import AzureBlobFile, AzureBlobFileSystem
from adlfs.tests.constants import ACCOUNT_NAME, CONN_STR, HOST, KEY, SAS_TOKEN
from adlfs.utils import __version__ as __version__


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


def get_expected_client_init_call(
    account_url,
    credential=None,
    location_mode="primary",
):
    call_kwargs = {
        "account_url": account_url,
        "user_agent": f"adlfs/{__version__}",
    }
    if credential is not None:
        call_kwargs["credential"] = credential
    if location_mode is not None:
        call_kwargs["_location_mode"] = location_mode
    return mock.call(mock.ANY, **call_kwargs)


def get_expected_client_from_connection_string_call(
    conn_str,
):
    return mock.call(conn_str=conn_str, user_agent=f"adlfs/{__version__}")


def assert_client_create_calls(
    mock_client_create_method,
    expected_create_call,
    expected_call_count=1,
):
    expected_call_args_list = [expected_create_call for _ in range(expected_call_count)]
    assert mock_client_create_method.call_args_list == expected_call_args_list


def ensure_no_api_calls_on_close(file_obj):
    # Marks the file-like object as closed to prevent any API calls during an invocation of
    # close(), which can occur during garbage collection or direct invocation.
    #
    # This is important for test cases where we do not want to make an API request whether:
    #
    # * The test would hang because Azurite is not configured to use SSL and adlfs always sets SSL
    #   for SDK clients created via their initializer.
    #
    # * The filesystem is configured to use the secondary location which can by-pass the location
    #   that Azurite is running.
    file_obj.closed = True


@pytest.mark.parametrize(
    "fs_kwargs,expected_client_init_call",
    [
        (
            {"account_name": ACCOUNT_NAME, "account_key": KEY},
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
                credential=KEY,
            ),
        ),
        (
            {"account_name": ACCOUNT_NAME, "credential": SAS_TOKEN},
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
                credential=SAS_TOKEN,
            ),
        ),
        (
            {"account_name": ACCOUNT_NAME, "sas_token": SAS_TOKEN},
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net?{SAS_TOKEN}",
            ),
        ),
        # Anonymous connection
        (
            {"account_name": ACCOUNT_NAME},
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
                location_mode=None,
            ),
        ),
        # Override host
        (
            {
                "account_name": ACCOUNT_NAME,
                "account_host": HOST,
                "sas_token": SAS_TOKEN,
            },
            get_expected_client_init_call(
                account_url=f"https://{HOST}?{SAS_TOKEN}",
            ),
        ),
        # Override location mode
        (
            {
                "account_name": ACCOUNT_NAME,
                "account_key": KEY,
                "location_mode": "secondary",
            },
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
                credential=KEY,
                location_mode="secondary",
            ),
        ),
        (
            {
                "account_name": ACCOUNT_NAME,
                "credential": SAS_TOKEN,
                "location_mode": "secondary",
            },
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
                credential=SAS_TOKEN,
                location_mode="secondary",
            ),
        ),
        (
            {
                "account_name": ACCOUNT_NAME,
                "sas_token": SAS_TOKEN,
                "location_mode": "secondary",
            },
            get_expected_client_init_call(
                account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net?{SAS_TOKEN}",
                location_mode="secondary",
            ),
        ),
    ],
)
def test_connect_initializer(
    storage: azure.storage.blob.BlobServiceClient,
    mock_service_client_init,
    fs_kwargs,
    expected_client_init_call,
):
    fs = AzureBlobFileSystem(skip_instance_cache=True, **fs_kwargs)
    assert_client_create_calls(mock_service_client_init, expected_client_init_call)

    f = AzureBlobFile(fs, "data/root/a/file.txt", mode="wb")
    f.connect_client()
    ensure_no_api_calls_on_close(f)
    assert_client_create_calls(
        mock_service_client_init,
        expected_client_init_call,
        expected_call_count=2,
    )


def test_connect_connection_str(
    storage: azure.storage.blob.BlobServiceClient, mock_from_connection_string
):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        skip_instance_cache=True,
    )
    expected_from_connection_str_call = get_expected_client_from_connection_string_call(
        conn_str=CONN_STR,
    )
    assert_client_create_calls(
        mock_from_connection_string, expected_from_connection_str_call
    )

    f = AzureBlobFile(fs, "data/root/a/file.txt", mode="rb")
    f.connect_client()
    assert_client_create_calls(
        mock_from_connection_string,
        expected_from_connection_str_call,
        expected_call_count=2,
    )
