import pytest

import adlfs


def test_connect(storage):
    adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )


@pytest.mark.xfail(reason="buggy implementation?")
def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    assert fs.ls("/") == ["root"]
    assert fs.ls("/root/a/") == ["file.txt"]


def test_open_file(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    f = fs.open("/root/a/file.txt")

    result = f.read()
    assert result == b"0123456789"
