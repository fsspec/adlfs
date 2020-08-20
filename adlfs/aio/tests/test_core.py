import asyncio
import docker
import dask.dataframe as dd
import pandas as pd
import pytest

import adlfs.aio.core as adlfs
from fsspec.asyn import get_loop

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
    adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )


@pytest.mark.asyncio
async def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    ## these are containers
    assert await fs.ls("") == ["data/"]
    assert await fs.ls("/") == ["data/"]
    assert await fs.ls(".") == ["data/"]

    ## these are top-level directories and files
    assert await fs.ls("data") == ["data/root/", "data/top_file.txt"]
    assert await fs.ls("/data") == ["data/root/", "data/top_file.txt"]

    # root contains files and directories
    assert await fs.ls("data/root") == [
        "data/root/a/",
        "data/root/b/",
        "data/root/c/",
        "data/root/rfile.txt",
    ]
    assert await fs.ls("data/root/") == [
        "data/root/a/",
        "data/root/b/",
        "data/root/c/",
        "data/root/rfile.txt",
    ]

    ## slashes are not not needed, but accepted
    assert await fs.ls("data/root/a") == ["data/root/a/file.txt"]
    assert await fs.ls("data/root/a/") == ["data/root/a/file.txt"]
    assert await fs.ls("/data/root/a") == ["data/root/a/file.txt"]
    assert await fs.ls("/data/root/a/") == ["data/root/a/file.txt"]

    ## file details
    assert await fs.ls("data/root/a/file.txt", detail=True) == [
        {"name": "data/root/a/file.txt", "size": 10, "type": "file"}
    ]

    # c has two files
    assert await fs.ls("data/root/c", detail=True) == [
        {"name": "data/root/c/file1.txt", "size": 10, "type": "file"},
        {"name": "data/root/c/file2.txt", "size": 10, "type": "file"},
    ]

    ## if not direct match is found throws error
    with pytest.raises(FileNotFoundError):
        await fs.ls("not-a-container")

    with pytest.raises(FileNotFoundError):
        await fs.ls("data/not-a-directory/")

    with pytest.raises(FileNotFoundError):
        await fs.ls("data/root/not-a-file.txt")


@pytest.mark.asyncio
async def test_info(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    container_info = await fs.info("data")
    assert container_info == {"name": "data/", "type": "directory", "size": 0}

    container2_info = await fs.info("data/root")
    assert container2_info == {"name": "data/root/", "type": "directory", "size": 0}

    dir_info = await fs.info("data/root/c")
    assert dir_info == {"name": "data/root/c/", "type": "directory", "size": 0}

    file_info = await fs.info("data/root/a/file.txt")
    assert file_info == {"name": "data/root/a/file.txt", "type": "file", "size": 10}


@pytest.mark.asyncio
async def test_find(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    ## just the directory name
    assert await fs.find("data/root/a") == ["data/root/a/file.txt"]  # NOQA
    assert await fs.find("data/root/a/") == ["data/root/a/file.txt"]  # NOQA

    assert await fs.find("data/root/c") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]
    assert await fs.find("data/root/c/") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    ## all files
    assert await fs.find("data/root") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]
    assert await fs.find("data/root", withdirs=False) == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]

    # all files and directories
    assert await fs.find("data/root", withdirs=True) == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]
    assert await fs.find("data/root/", withdirs=True) == [
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
    assert await fs.find("data/missing") == []


# @pytest.mark.xfail
# def test_find_missing(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         account_name=storage.account_name, connection_string=CONN_STR
#     )
#     assert await fs.find("data/roo") == []

@pytest.mark.asyncio
async def test_glob(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    ## just the directory name
    assert await fs.glob("data/root") == ["data/root"]

    # top-level contents of a directory
    assert await fs.glob("data/root/") == [
        "data/root/a",
        "data/root/b",
        "data/root/c",
        "data/root/rfile.txt",
    ]
    assert await fs.glob("data/root/*") == [
        "data/root/a",
        "data/root/b",
        "data/root/c",
        "data/root/rfile.txt",
    ]

    assert await fs.glob("data/root/b/*") == ["data/root/b/file.txt"]  # NOQA
    assert await fs.glob("data/root/b/**") == ["data/root/b/file.txt"]  # NOQA

    ## across directories
    assert await fs.glob("data/root/*/file.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
    ]

    ## regex match
    assert await fs.glob("data/root/*/file[0-9].txt") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    ## text files
    assert await fs.glob("data/root/*/file*.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]

    ## all text files
    assert await fs.glob("data/**/*.txt") == [
        "data/root/a/file.txt",
        "data/root/b/file.txt",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]

    ## all files
    assert await fs.glob("data/root/**") == [
        "data/root/a",
        "data/root/a/file.txt",
        "data/root/b",
        "data/root/b/file.txt",
        "data/root/c",
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
        "data/root/rfile.txt",
    ]
    assert await fs.glob("data/roo**") == [
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
    assert await fs.glob("data/missing/*") == []


def test_open_file(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    f = fs.open("/data/root/a/file.txt")

    result = f.read()
    assert result == b"0123456789"


@pytest.mark.asyncio
async def test_rm(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    await fs.rm("/data/root/a/file.txt")

    with pytest.raises(FileNotFoundError):
        await fs.ls("/data/root/a/file.txt")


@pytest.mark.asyncio
async def test_rm_recursive(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    assert "data/root/c/" in await fs.ls("/data/root")
    assert await fs.ls("data/root/c") == [
        "data/root/c/file1.txt",
        "data/root/c/file2.txt",
    ]
    await fs.rm("data/root/c", recursive=True)
    assert "data/root/c/" not in await fs.ls("/data/root")

    with pytest.raises(FileNotFoundError):
        await fs.ls("data/root/c")

@pytest.mark.asyncio
async def test_mkdir_rmdir(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )
        
    await fs.mkdir("new-container")
    assert "new-container/" in await fs.ls("")
    assert await fs.ls("new-container") == []

    async with adlfs.AzureBlobFile(fs=fs, path="new-container/file.txt", mode="wb") as f:
        await f.write(b"0123456789")

    async with adlfs.AzureBlobFile(fs, "new-container/dir/file.txt", "wb") as f:
        await f.write(b"0123456789")

    async with fs.open("new-container/dir/file.txt", "wb") as f:
        await f.write(b"0123456789")

    # Check to verify you can skip making a directory if the container
    # already exists, but still create a file in that directory
    await fs.mkdir("new-container/dir/file.txt", exists_ok=True)
    assert "new-container/" in await fs.ls("")

    await fs.mkdir("new-container/file2.txt", exists_ok=True)
    async with fs.open("new-container/file2.txt", "wb") as f:
        await f.write(b"0123456789")
    assert "new-container/file2.txt" in await fs.ls("new-container")

    await fs.mkdir("new-container/dir/file2.txt", exists_ok=True)
    async with fs.open("new-container/dir/file2.txt", "wb") as f:
        await f.write(b"0123456789")
    print(await fs.ls("new-container"))
    assert "new-container/dir/file2.txt" in await fs.ls("new-container/dir")

    # Also verify you can make a nested directory structure
    await fs.mkdir("new-container/dir2/file.txt", exists_ok=True)
    async with fs.open("new-container/dir2/file.txt", "wb") as f:
        await f.write(b"0123456789")
    assert "new-container/dir2/file.txt" in await fs.ls("new-container/dir2")
    await fs.rm("new-container/dir2", recursive=True)

    await fs.rm("new-container/dir", recursive=True)
    assert await fs.ls("new-container") == [
        "new-container/file.txt",
        "new-container/file2.txt",
    ]

    await fs.rm("new-container/file.txt")
    await fs.rm("new-container/file2.txt")
    await fs.rmdir("new-container")

    assert "new-container/" not in await fs.ls("")


@pytest.mark.asyncio
async def test_mkdir_rm_recursive(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    await fs.mkdir("test_mkdir_rm_recursive")
    assert "test_mkdir_rm_recursive/" in await fs.ls("")

    async with fs.open("test_mkdir_rm_recursive/file.txt", "wb") as f:
        await f.write(b"0123456789")

    async with fs.open("test_mkdir_rm_recursive/dir/file.txt", "wb") as f:
        await f.write(b"ABCD")

    async with fs.open("test_mkdir_rm_recursive/dir/file2.txt", "wb") as f:
        await f.write(b"abcdef")

    assert await fs.find("test_mkdir_rm_recursive") == [
        "test_mkdir_rm_recursive/dir/file.txt",
        "test_mkdir_rm_recursive/dir/file2.txt",
        "test_mkdir_rm_recursive/file.txt",
    ]

    await fs.rm("test_mkdir_rm_recursive", recursive=True)

    assert "test_mkdir_rm_recursive/" not in await fs.ls("")
    assert await fs.find("test_mkdir_rm_recursive") == []


@pytest.mark.asyncio
async def test_deep_paths(storage):
    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    await fs.mkdir("test_deep")
    assert "test_deep/" in await fs.ls("")

    async with fs.open("test_deep/a/b/c/file.txt", "wb") as f:
        await f.write(b"0123456789")

    assert await fs.ls("test_deep") == ["test_deep/a/"]
    assert await fs.ls("test_deep/") == ["test_deep/a/"]
    assert await fs.ls("test_deep/a") == ["test_deep/a/b/"]
    assert await fs.ls("test_deep/a/") == ["test_deep/a/b/"]
    assert await fs.find("test_deep") == ["test_deep/a/b/c/file.txt"]
    assert await fs.find("test_deep/") == ["test_deep/a/b/c/file.txt"]
    assert await fs.find("test_deep/a") == ["test_deep/a/b/c/file.txt"]
    assert await fs.find("test_deep/a/") == ["test_deep/a/b/c/file.txt"]

    await fs.rm("test_deep", recursive=True)

    assert "test_deep/" not in await fs.ls("")
    assert await fs.find("test_deep") == []


@pytest.mark.asyncio
async def test_large_blob(storage):
    import tempfile
    import hashlib
    import io
    import shutil
    from pathlib import Path

    fs = adlfs.AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )

    # create a 20MB byte array, ensure it's larger than blocksizes to force a
    # chuncked upload
    blob_size = 120_000_000
    assert blob_size > fs.blocksize
    assert blob_size > adlfs.AzureBlobFile.DEFAULT_BLOCK_SIZE

    data = b"1" * blob_size
    _hash = hashlib.md5(data)
    expected = _hash.hexdigest()

    # create container
    await fs.mkdir("chunk-container")

    # upload the data using fs.open
    path = "chunk-container/large-blob.bin"
    async with fs.open(path, "wb") as dst:
        await dst.write(data)

    assert await fs.exists(path)
    assert await fs.size(path) == blob_size

    del data

    # download with fs.open
    bio = io.BytesIO()
    async with fs.open(path, "rb") as src:
        await shutil.copyfileobj(src, bio)

    # read back the data and calculate md5
    bio.seek(0)
    data = bio.read()
    _hash = hashlib.md5(data)
    result = _hash.hexdigest()

    assert expected == result

    # do the same but using upload/download and a tempdir
    # path = path = "chunk-container/large_blob2.bin"
    # async with tempfile.TemporaryDirectory() as td:
    #     local_blob: Path = Path(td) / "large_blob2.bin"
    #     async with local_blob.open("wb") as fo:
    #         await fo.write(data)
    #     assert local_blob.exists()
    #     assert local_blob.stat().st_size == blob_size

    #     fs.upload(str(local_blob), path)
    #     assert await fs.exists(path)
    #     assert fs.size(path) == blob_size

    #     # download now
    #     local_blob.unlink()
    #     fs.download(path, str(local_blob))
    #     assert local_blob.exists()
    #     assert local_blob.stat().st_size == blob_size


# def test_dask_parquet(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         account_name=storage.account_name, connection_string=CONN_STR
#     )
#     fs.mkdir("test")
#     STORAGE_OPTIONS = {
#         "account_name": "devstoreaccount1",
#         "connection_string": CONN_STR,
#     }
#     df = pd.DataFrame(
#         {
#             "col1": [1, 2, 3, 4],
#             "col2": [2, 4, 6, 8],
#             "index_key": [1, 1, 2, 2],
#             "partition_key": [1, 1, 2, 2],
#         }
#     )

#     dask_dataframe = dd.from_pandas(df, npartitions=1)
#     dask_dataframe.to_parquet(
#         "abfs://test/test_group.parquet",
#         storage_options=STORAGE_OPTIONS,
#         engine="pyarrow",
#     )
#     fs = adlfs.AzureBlobFileSystem(**STORAGE_OPTIONS)
#     assert fs.ls("test/test_group.parquet") == [
#         "test/test_group.parquet/_common_metadata",
#         "test/test_group.parquet/_metadata",
#         "test/test_group.parquet/part.0.parquet",
#     ]
