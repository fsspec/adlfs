import asyncio
import docker
import dask.dataframe as dd
from fsspec.implementations.local import LocalFileSystem
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from adlfs import AzureBlobFileSystem, AzureBlobFile


URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # NOQA
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"  # NOQA


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    print("Starting azurite docker container")
    client = docker.from_env()
    azurite = client.containers.run(
        "mcr.microsoft.com/azure-storage/azurite", ports={"10000": "10000"}, detach=True
    )
    yield azurite
    print("Teardown azurite docker container")
    azurite.stop()


def test_connect(storage):
    AzureBlobFileSystem(account_name=storage.account_name, connection_string=CONN_STR)


def test_ls(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    ## these are containers
    assert fs.ls("") == ["data/"]
    assert fs.ls("/") == ["data/"]
    assert fs.ls(".") == ["data/"]

    ## these are top-level directories and files
    assert fs.ls("data") == ["data/root/", "data/top_file.txt"]
    assert fs.ls("/data") == ["data/root/", "data/top_file.txt"]

    # root contains files and directories
    assert fs.ls("data/root") == [
        "data/root/a/",
        "data/root/b/",
        "data/root/c/",
        "data/root/rfile.txt",
    ]
    assert fs.ls("data/root/") == [
        "data/root/a/",
        "data/root/b/",
        "data/root/c/",
        "data/root/rfile.txt",
    ]

    ## slashes are not not needed, but accepted
    assert fs.ls("data/root/a") == ["data/root/a/file.txt"]
    assert fs.ls("data/root/a/") == ["data/root/a/file.txt"]
    assert fs.ls("/data/root/a") == ["data/root/a/file.txt"]
    assert fs.ls("/data/root/a/") == ["data/root/a/file.txt"]

    ## file details
    assert fs.ls("data/root/a/file.txt", detail=True) == [
        {"name": "data/root/a/file.txt", "size": 10, "type": "file"}
    ]

    # c has two files
    assert fs.ls("data/root/c", detail=True) == [
        {"name": "data/root/c/file1.txt", "size": 10, "type": "file"},
        {"name": "data/root/c/file2.txt", "size": 10, "type": "file"},
    ]

    ## if not direct match is found throws error
    with pytest.raises(FileNotFoundError):
        fs.ls("not-a-container")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/not-a-directory/")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/root/not-a-file.txt")


def test_info(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    container_info = fs.info("data")
    assert container_info == {"name": "data/", "type": "directory", "size": 0}

    container2_info = fs.info("data/root")
    assert container2_info == {"name": "data/root/", "type": "directory", "size": 0}

    dir_info = fs.info("data/root/c")
    assert dir_info == {"name": "data/root/c/", "type": "directory", "size": 0}

    file_info = fs.info("data/root/a/file.txt")
    assert file_info == {"name": "data/root/a/file.txt", "type": "file", "size": 10}


def test_find(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    ## just the directory name
    assert fs.find("data/root/a") == ["data/root/a/file.txt"]  # NOQA
    assert fs.find("data/root/a/") == ["data/root/a/file.txt"]  # NOQA

    assert fs.find("data/root/c") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]
    assert fs.find("data/root/c/") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    ## all files
    assert fs.find("data/root") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]
    assert fs.find("data/root", withdirs=False) == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]

    # all files and directories
    assert fs.find("data/root", withdirs=True) == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]
    assert fs.find("data/root/", withdirs=True) == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]

    ## missing
    assert fs.find("data/missing") == []


@pytest.mark.xfail
def test_find_missing(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    assert fs.find("data/roo") == []


def test_glob(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    ## just the directory name
    assert fs.glob("data/root") == ["data/root"]

    # top-level contents of a directory
    assert fs.glob("data/root/") == [
        "data/root/a",
        "data/root/b",
        "data/root/c",
        "data/root/rfile.txt",
    ]
    assert fs.glob("data/root/*") == [
        "data/root/a",
        "data/root/b",
        "data/root/c",
        "data/root/rfile.txt",
    ]

    assert fs.glob("data/root/b/*") == ["data/root/b/file.txt"]  # NOQA
    assert fs.glob("data/root/b/**") == ["data/root/b/file.txt"]  # NOQA

    ## across directories
    assert fs.glob("data/root/*/file.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
    ]

    ## regex match
    assert fs.glob("data/root/*/file[0-9].txt") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    ## text files
    assert fs.glob("data/root/*/file*.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    ## all text files
    assert fs.glob("data/**/*.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]

    ## all files
    assert fs.glob("data/root/**") == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]
    assert fs.glob("data/roo**") == [
        "data/root",
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]

    ## missing
    assert fs.glob("data/missing/*") == []


def test_open_file(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    f = fs.open("/data/root/a/file.txt")

    result = f.read()
    assert result == b"0123456789"


def test_rm(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    fs.rm("/data/root/a/file.txt")

    with pytest.raises(FileNotFoundError):
        fs.ls("/data/root/a/file.txt", refresh=True)


def test_rm_recursive(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    assert "data/root/c/" in fs.ls("/data/root")

    assert fs.ls("data/root/c") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]
    fs.rm("data/root/c", recursive=True)
    assert "data/root/c/" not in fs.ls("/data/root")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/root/c")


def test_mkdir_rmdir(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR,
    )

    fs.mkdir("new-container")
    assert "new-container/" in fs.ls("")
    assert fs.ls("new-container") == []

    with fs.open(path="new-container/file.txt", mode="wb") as f:
        f.write(b"0123456789")

    with fs.open("new-container/dir/file.txt", "wb") as f:
        f.write(b"0123456789")

    with fs.open("new-container/dir/file2.txt", "wb") as f:
        f.write(b"0123456789")

    # Check to verify you can skip making a directory if the container
    # already exists, but still create a file in that directory
    fs.mkdir("new-container/dir/file.txt", exists_ok=True)
    assert "new-container/" in fs.ls("")

    fs.mkdir("new-container/file2.txt", exists_ok=True)
    assert "new-container/file2.txt" in fs.ls("new-container")

    # Test to verify that the file contains expected contents
    with fs.open("new-container/file2.txt", "rb") as f:
        outfile = f.read()
    assert outfile == b""

    # Check that trying to overwrite an existing nested file in append mode works as expected
    fs.mkdir("new-container/dir/file2.txt", exists_ok=True)
    assert "new-container/dir/file2.txt" in fs.ls("new-container/dir")

    # Also verify you can make a nested directory structure
    fs.mkdir("new-container/dir2/file.txt", exists_ok=True)
    with fs.open("new-container/dir2/file.txt", "wb") as f:
        f.write(b"0123456789")
    assert "new-container/dir2/file.txt" in fs.ls("new-container/dir2")
    fs.rm("new-container/dir2", recursive=True)

    fs.rm("new-container/dir", recursive=True)
    assert fs.ls("new-container") == [
        "new-container/file.txt",
        "new-container/file2.txt",
    ]

    fs.rm("new-container/file.txt")
    fs.rm("new-container/file2.txt")
    fs.rmdir("new-container")

    assert "new-container/" not in fs.ls("")


@pytest.mark.skip
def test_append_operation(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("new-container")
    fs.mkdir("new-container/dir")

    # Check that appending to an existing file works as expected
    with fs.open("new-container/file2.txt", "ab") as f:
        f.write(b"0123456789")
    with fs.open("new-container/dir/file2.txt", "ab") as f:
        f.write(b"0123456789")
    with fs.open("new-container/dir/file2.txt", "ab") as f:
        f.write(b"0123456789")
    with fs.open("new-container/dir/file2.txt", "rb") as f:
        outfile = f.read()
    assert outfile == b"01234567890123456789"

    fs.rm("new-container", recursive=True)


def test_mkdir_rm_recursive(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    fs.mkdir("test_mkdir_rm_recursive")
    assert "test_mkdir_rm_recursive/" in fs.ls("")

    with fs.open("test_mkdir_rm_recursive/file.txt", "wb") as f:
        f.write(b"0123456789")

    with fs.open("test_mkdir_rm_recursive/dir/file.txt", "wb") as f:
        f.write(b"ABCD")

    with fs.open("test_mkdir_rm_recursive/dir/file2.txt", "wb") as f:
        f.write(b"abcdef")

    assert fs.find("test_mkdir_rm_recursive") == [
        "test_mkdir_rm_recursive/dir/file.txt",
        "test_mkdir_rm_recursive/dir/file2.txt",
        "test_mkdir_rm_recursive/file.txt",
    ]

    fs.rm("test_mkdir_rm_recursive", recursive=True)

    assert "test_mkdir_rm_recursive/" not in fs.ls("")
    assert fs.find("test_mkdir_rm_recursive") == []


def test_deep_paths(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    fs.mkdir("test_deep")
    assert "test_deep/" in fs.ls("")

    with fs.open("test_deep/a/b/c/file.txt", "wb") as f:
        f.write(b"0123456789")

    assert fs.ls("test_deep") == ["test_deep/a/"]
    assert fs.ls("test_deep/") == ["test_deep/a/"]
    assert fs.ls("test_deep/a") == ["test_deep/a/b/"]
    assert fs.ls("test_deep/a/") == ["test_deep/a/b/"]
    assert fs.find("test_deep") == ["test_deep/a/b/c/file.txt"]
    assert fs.find("test_deep/") == ["test_deep/a/b/c/file.txt"]
    assert fs.find("test_deep/a") == ["test_deep/a/b/c/file.txt"]
    assert fs.find("test_deep/a/") == ["test_deep/a/b/c/file.txt"]

    fs.rm("test_deep", recursive=True)

    assert "test_deep/" not in fs.ls("")
    assert fs.find("test_deep") == []


def test_large_blob(storage):
    import tempfile
    import hashlib
    import io
    import shutil
    from pathlib import Path

    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    # create a 20MB byte array, ensure it's larger than blocksizes to force a
    # chuncked upload
    blob_size = 120_000_000
    assert blob_size > fs.blocksize
    assert blob_size > AzureBlobFile.DEFAULT_BLOCK_SIZE

    data = b"1" * blob_size
    _hash = hashlib.md5(data)
    expected = _hash.hexdigest()

    # create container
    fs.mkdir("chunk-container")

    # upload the data using fs.open
    path = "chunk-container/large-blob.bin"
    with fs.open(path, "wb") as dst:
        dst.write(data)

    assert fs.exists(path)
    assert fs.size(path) == blob_size

    del data

    # download with fs.open
    bio = io.BytesIO()
    with fs.open(path, "rb") as src:
        shutil.copyfileobj(src, bio)

    # read back the data and calculate md5
    bio.seek(0)
    data = bio.read()
    _hash = hashlib.md5(data)
    result = _hash.hexdigest()

    assert expected == result

    # do the same but using upload/download and a tempdir
    path = path = "chunk-container/large_blob2.bin"
    with tempfile.TemporaryDirectory() as td:
        local_blob: Path = Path(td) / "large_blob2.bin"
        with local_blob.open("wb") as fo:
            fo.write(data)
        assert local_blob.exists()
        assert local_blob.stat().st_size == blob_size

        fs.upload(str(local_blob), path)
        assert fs.exists(path)
        assert fs.size(path) == blob_size

        # download now
        local_blob.unlink()
        fs.download(path, str(local_blob))
        assert local_blob.exists()
        assert local_blob.stat().st_size == blob_size


def test_dask_parquet(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("test")
    STORAGE_OPTIONS = {
        "account_name": "devstoreaccount1",
        "connection_string": CONN_STR,
    }
    df = pd.DataFrame(
        {
            "col1": [1, 2, 3, 4],
            "col2": [2, 4, 6, 8],
            "index_key": [1, 1, 2, 2],
            "partition_key": [1, 1, 2, 2],
        }
    )

    dask_dataframe = dd.from_pandas(df, npartitions=1)
    for protocol in ["abfs", "az"]:
        dask_dataframe.to_parquet(
            "{}://test/test_group.parquet".format(protocol),
            storage_options=STORAGE_OPTIONS,
            engine="pyarrow",
        )

        fs = AzureBlobFileSystem(**STORAGE_OPTIONS)
        assert fs.ls("test/test_group.parquet") == [
            "test/test_group.parquet/_common_metadata",
            "test/test_group.parquet/_metadata",
            "test/test_group.parquet/part.0.parquet",
        ]
        fs.rm("test/test_group.parquet")

    df_test = dd.read_parquet(
        "abfs://test/test_group.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df, df_test)

    A = np.random.randint(0, 100, size=(10000, 4))
    df2 = pd.DataFrame(data=A, columns=list("ABCD"))
    ddf2 = dd.from_pandas(df2, npartitions=4)
    dd.to_parquet(
        ddf2,
        "abfs://test/test_group2.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    )
    assert fs.ls("test/test_group2.parquet") == [
        "test/test_group2.parquet/_common_metadata",
        "test/test_group2.parquet/_metadata",
        "test/test_group2.parquet/part.0.parquet",
        "test/test_group2.parquet/part.1.parquet",
        "test/test_group2.parquet/part.2.parquet",
        "test/test_group2.parquet/part.3.parquet",
    ]
    df2_test = dd.read_parquet(
        "abfs://test/test_group2.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df2, df2_test)

    a = np.full(shape=(10000, 1), fill_value=1)
    b = np.full(shape=(10000, 1), fill_value=2)
    c = np.full(shape=(10000, 1), fill_value=3)
    d = np.full(shape=(10000, 1), fill_value=4)
    B = np.concatenate((a, b, c, d), axis=1)
    df3 = pd.DataFrame(data=B, columns=list("ABCD"))
    ddf3 = dd.from_pandas(df3, npartitions=4)
    dd.to_parquet(
        ddf3,
        "abfs://test/test_group3.parquet",
        partition_on=["A", "B"],
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    )
    assert fs.glob("test/test_group3.parquet/*") == [
        "test/test_group3.parquet/A=1",
        "test/test_group3.parquet/_common_metadata",
        "test/test_group3.parquet/_metadata",
    ]
    df3_test = dd.read_parquet(
        "abfs://test/test_group3.parquet",
        filters=[("A", "=", 1)],
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    df3_test = df3_test[["A", "B", "C", "D"]]
    df3_test = df3_test[["A", "B", "C", "D"]].astype(int)
    assert_frame_equal(df3, df3_test)

    A = np.random.randint(0, 100, size=(10000, 4))
    df4 = pd.DataFrame(data=A, columns=list("ABCD"))
    ddf4 = dd.from_pandas(df4, npartitions=4)
    dd.to_parquet(
        ddf4,
        "abfs://test/test_group4.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
        flavor="spark",
        write_statistics=False,
    )
    fs.rmdir("test/test_group4.parquet/_common_metadata", recursive=True)
    fs.rmdir("test/test_group4.parquet/_metadata", recursive=True)
    fs.rm("test/test_group4.parquet/_common_metadata")
    fs.rm("test/test_group4.parquet/_metadata")
    assert fs.ls("test/test_group4.parquet") == [
        "test/test_group4.parquet/part.0.parquet",
        "test/test_group4.parquet/part.1.parquet",
        "test/test_group4.parquet/part.2.parquet",
        "test/test_group4.parquet/part.3.parquet",
    ]
    df4_test = dd.read_parquet(
        "abfs://test/test_group4.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df4, df4_test)

    A = np.random.randint(0, 100, size=(10000, 4))
    df5 = pd.DataFrame(data=A, columns=list("ABCD"))
    ddf5 = dd.from_pandas(df5, npartitions=4)
    dd.to_parquet(
        ddf5,
        "abfs://test/test group5.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    )
    assert fs.ls("test/test group5.parquet") == [
        "test/test group5.parquet/_common_metadata",
        "test/test group5.parquet/_metadata",
        "test/test group5.parquet/part.0.parquet",
        "test/test group5.parquet/part.1.parquet",
        "test/test group5.parquet/part.2.parquet",
        "test/test group5.parquet/part.3.parquet",
    ]
    df5_test = dd.read_parquet(
        "abfs://test/test group5.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df5, df5_test)


def test_put_empty_file(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    lfs = LocalFileSystem()

    fs.mkdir("putdir")
    with open("sample.txt", "wb") as f:
        f.write(b"")
    fs.put("sample.txt", "putdir/sample.txt")
    fs.get("putdir/sample.txt", "sample2.txt")

    with open("sample.txt", "rb") as f:
        f1 = f.read()
    with open("sample2.txt", "rb") as f:
        f2 = f.read()
    assert f1 == f2

    lfs.rm("sample.txt")
    lfs.rm("sample2.txt")
    fs.rm("putdir", recursive=True)


@pytest.mark.skip
def test_isdir(storage):
    pass
