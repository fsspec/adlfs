import pickle

from adlfs import AzureBlobFileSystem
from adlfs.tests.constants import CONN_STR


def test_fs_pickling(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
        kwarg1="some_value",
    )
    fs2: AzureBlobFileSystem = pickle.loads(pickle.dumps(fs))
    assert "data" in fs.ls("")
    assert "data" in fs2.ls("")
    assert fs2.kwargs["kwarg1"] == "some_value"


def test_blob_pickling(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs2: AzureBlobFileSystem = pickle.loads(pickle.dumps(fs))
    blob = fs2.open("data/root/a/file.txt")
    assert blob.read() == b"0123456789"
    blob2 = pickle.loads(pickle.dumps(blob))
    blob2.seek(0)
    assert blob2.read() == b"0123456789"
