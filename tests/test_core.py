import adlfs

def test_connect(storage):
    adlfs.AzureBlobFileSystem(
        "data",
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )


def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(
        "data",
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    assert fs.ls("") == ["root/a/file.txt", "root/b/file.txt"]
    assert fs.ls("root/a/") == ["root/a/file.txt"]


def test_open_file(storage):
    fs = adlfs.AzureBlobFileSystem(
        "data",
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    f = fs.open("root/a/file.txt")

    result = f.read()
    assert result == b"0123456789"

