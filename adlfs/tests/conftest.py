import pytest

from azure.storage.blob import BlockBlobService


URL = "127.0.0.1:10000"
USERNAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


data = b"0123456789"


@pytest.fixture
def storage():
    """
    Create blob using azurite.
    """
    bbs = BlockBlobService(is_emulated=True)
    bbs.create_container('data', timeout=1)

    bbs.create_blob_from_bytes("data", "/root/a/file.txt", data)
    bbs.create_blob_from_bytes("data", "/root/b/file.txt", data)
    yield bbs
