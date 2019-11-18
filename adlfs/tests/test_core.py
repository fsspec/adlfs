import pytest

import adlfs


def test_connect(storage):
    adlfs.AzureBlobFileSystem(storage.account_name, "data", storage.account_key)


@pytest.mark.xfail(reason="buggy implementation")
def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(storage.account_name, "data", storage.account_key)
    assert fs.ls("/") == ["root"]
    assert fs.ls("/root/a/") == ["file.txt"]
