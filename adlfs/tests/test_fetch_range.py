from adlfs import AzureBlobFileSystem
from adlfs.tests.constants import CONN_STR


def test_fetch_entire_blob(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )
    blob = fs.open("data/top_file.txt")
    assert len(blob._fetch_range(start=0, length=10)) == 10


def test_fetch_first_half(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )
    blob = fs.open("data/top_file.txt")
    assert len(blob._fetch_range(start=0, end=5)) == 5


def test_fetch_second_half(storage):
    # Verify if length extends beyond the end of file, truncate the read
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )
    blob = fs.open("data/top_file.txt")
    assert len(blob._fetch_range(start=5, end=10)) == 5


def test_fetch_middle(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )
    blob = fs.open("data/top_file.txt")
    assert len(blob._fetch_range(start=2, end=7)) == 5


def test_fetch_length_is_none(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )
    blob = fs.open("data/top_file.txt")
    assert len(blob._fetch_range(start=2, end=None)) == 8
