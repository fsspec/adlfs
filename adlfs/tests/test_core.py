import adlfs


def test_connect(storage):
    fs = adlfs.AzureBlobFileSystem(storage.account_name, "data", storage.account_key, is_emulated=True)


def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(storage.account_name, "data", storage.account_key, is_emulated=True)
    assert fs.ls("/") == ["a", "b"]
    assert fs.ls("a") == ["file.txt"]
