import pytest

from azure.storage.blob import BlockBlobService


URL = "127.0.0.1:10000"
USERNAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
data = b"0123456789"


def pytest_addoption(parser):
    parser.addoption(
        "--host", action="store", default="127.0.0.1:10000", help="Host running azurite."
    )


@pytest.fixture
def host(request):
    print("host:", request.config.getoption("--host"))
    return request.config.getoption("--host")


@pytest.fixture
def storage(host):
    """
    Create blob using azurite.
    """
    bbs = BlockBlobService(
        account_name=USERNAME,
        account_key=KEY,
        custom_domain=f"http://{host}/devstoreaccount1",
    )
    bbs.create_container("data", timeout=1)

    bbs.create_blob_from_bytes("data", "/root/a/file.txt", data)
    bbs.create_blob_from_bytes("data", "/root/b/file.txt", data)
    yield bbs
