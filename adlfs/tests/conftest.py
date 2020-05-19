import pytest

from azure.storage.blob import BlobServiceClient


URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA
data = b"0123456789"


def pytest_addoption(parser):
    parser.addoption(
        "--host",
        action="store",
        default="127.0.0.1:10000",
        help="Host running azurite.",
    )


@pytest.fixture(scope="session")
def host(request):
    print("host:", request.config.getoption("--host"))
    return request.config.getoption("--host")


@pytest.fixture(scope="session")
def storage(host):
    """
    Create blob using azurite.
    """

    conn_str = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA

    bbs = BlobServiceClient.from_connection_string(conn_str=conn_str)
    bbs.create_container("data")
    container_client = bbs.get_container_client(container="data")
    container_client.upload_blob("top_file.txt", data)
    container_client.upload_blob("root/rfile.txt", data)
    container_client.upload_blob("root/a/file.txt", data)
    container_client.upload_blob("root/b/file.txt", data)
    container_client.upload_blob("root/c/file1.txt", data)
    container_client.upload_blob("root/c/file2.txt", data)
    yield bbs
