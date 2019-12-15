import pytest

import adlfs


def test_connect(storage):
    adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )


def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    assert fs.ls("") == ["root/a/", "root/b/", "root/c/"]
    assert fs.ls("root/") == ["root/a/", "root/b/", "root/c/"]
    assert fs.ls("root") == ["root/a/", "root/b/", "root/c/"]
    assert fs.ls("root/a/") == ["root/a/file.txt"]
    assert fs.ls("root/a") == ["root/a/file.txt"]

    assert fs.ls("root/a/file.txt", detail=True) == [
        {
            "name": "root/a/file.txt",
            "size": 10,
            "container_name": "data",
            "type": "file",
        }
    ]
    assert fs.ls("root/a", detail=True) == [
        {
            "name": "root/a/file.txt",
            "size": 10,
            "container_name": "data",
            "type": "file",
        }
    ]
    assert fs.ls("root/a/", detail=True) == [
        {
            "name": "root/a/file.txt",
            "size": 10,
            "container_name": "data",
            "type": "file",
        }
    ]


def test_info(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    assert fs.info("root/a/file.txt")["name"] == "root/a/file.txt"
    assert fs.info("root/a/file.txt")["container_name"] == "data"
    assert fs.info("root/a/file.txt")["type"] == "file"
    assert fs.info("root/a/file.txt")["size"] == 10
    # assert fs.info('root/a')['container_name'] == 'data'


def test_glob(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        "data",
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    assert fs.glob("root/a/file.txt") == ["root/a/file.txt"]
    assert fs.glob("root/a/") == ["root/a/file.txt"]
    assert fs.glob("root/a") == ["root/a"]
    assert fs.glob("root/") == ["root/a", "root/b", "root/c"]
    assert fs.glob("root/*") == ["root/a", "root/b", "root/c"]
    assert fs.glob("root/c/*.txt") == ["root/c/file1.txt", "root/c/file2.txt"]


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
