import docker
import pytest

import adlfs

URL = "http://127.0.0.1:10000"
ACCOUNT_NAME = "devstoreaccount1"
KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONN_STR = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={KEY};BlobEndpoint={URL}/{ACCOUNT_NAME};"

@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    print("Spawning docker container")
    client = docker.from_env()
    azurite = client.containers.run(
        "mcr.microsoft.com/azure-storage/azurite", ports={"10000": "10000"}, detach=True
    )
    yield azurite
    print("Teardown azurite docker container")
    azurite.stop()


def test_connect(storage):
    adlfs.AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR,
    )


def test_ls(storage):
    fs =  adlfs.AzureBlobFileSystem(
        account_name=storage.account_name,
        connection_string=CONN_STR
        )

    ## these are containers
    assert fs.ls("") == ["data/"]
    assert fs.ls("/") == ["data/"]

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

    ## c has two files
    assert fs.ls("data/root/c", detail=True) == [
        {"name": "data/root/c/file1.txt", "size": 10, "type": "file"},
        {"name": "data/root/c/file2.txt", "size": 10, "type": "file"},
    ]

#     ## if not direct match is found throws error
#     with pytest.raises(FileNotFoundError):
#         fs.ls("not-a-container")

#     with pytest.raises(FileNotFoundError):
#         fs.ls("data/not-a-directory/")

#     with pytest.raises(FileNotFoundError):
#         fs.ls("data/root/not-a-file.txt")


# def test_info(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         storage.account_name,
#         storage.account_key,
#         custom_domain=f"http://{storage.primary_endpoint}",
#     )

#     container_info = fs.info("data")
#     assert container_info == {"name": "data/", "type": "directory", "size": 0}

#     dir_info = fs.info("data/root/c")
#     assert dir_info == {"name": "data/root/c/", "type": "directory", "size": 0}
#     file_info = fs.info("data/root/a/file.txt")
#     assert file_info == {"name": "data/root/a/file.txt", "type": "file", "size": 10}


# def test_glob(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         storage.account_name,
#         storage.account_key,
#         custom_domain=f"http://{storage.primary_endpoint}",
#     )

#     ## just the directory name
#     assert fs.glob("data/root") == ["data/root"]
#     ## top-level contents of a directory
#     assert fs.glob("data/root/") == [
#         "data/root/a",
#         "data/root/b",
#         "data/root/c",
#         "data/root/rfile.txt",
#     ]
#     assert fs.glob("data/root/*") == [
#         "data/root/a",
#         "data/root/b",
#         "data/root/c",
#         "data/root/rfile.txt",
#     ]

#     assert fs.glob("data/root/b/*") == ["data/root/b/file.txt"]

#     ## across directories
#     assert fs.glob("data/root/*/file.txt") == [
#         "data/root/a/file.txt",
#         "data/root/b/file.txt",
#     ]

#     ## regex match
#     assert fs.glob("data/root/*/file[0-9].txt") == [
#         "data/root/c/file1.txt",
#         "data/root/c/file2.txt",
#     ]

#     ## text files
#     assert fs.glob("data/root/*/file*.txt") == [
#         "data/root/a/file.txt",
#         "data/root/b/file.txt",
#         "data/root/c/file1.txt",
#         "data/root/c/file2.txt",
#     ]

#     ## all text files
#     assert fs.glob("data/**/*.txt") == [
#         "data/root/a/file.txt",
#         "data/root/b/file.txt",
#         "data/root/c/file1.txt",
#         "data/root/c/file2.txt",
#         "data/root/rfile.txt",
#     ]

#     ## missing
#     assert fs.glob("data/missing/*") == []


# def test_open_file(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         storage.account_name,
#         storage.account_key,
#         custom_domain=f"http://{storage.primary_endpoint}",
#     )
#     f = fs.open("/data/root/a/file.txt")

#     result = f.read()
#     assert result == b"0123456789"


# def test_rm(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         storage.account_name,
#         storage.account_key,
#         custom_domain=f"http://{storage.primary_endpoint}",
#     )

#     fs.rm("/data/root/a/file.txt")

#     with pytest.raises(FileNotFoundError):
#         fs.ls("/data/root/a/file.txt")


# def test_mkdir_rmdir(storage):
#     fs = adlfs.AzureBlobFileSystem(
#         storage.account_name,
#         storage.account_key,
#         custom_domain=f"http://{storage.primary_endpoint}",
#     )

#     fs.mkdir("new-container")
#     assert "new-container/" in fs.ls("")

#     with fs.open("new-container/file.txt", "wb") as f:
#         f.write(b"0123456789")

#     with fs.open("new-container/dir/file.txt", "wb") as f:
#         f.write(b"0123456789")

#     with fs.open("new-container/dir/file.txt", "wb") as f:
#         f.write(b"0123456789")

#     # Check to verify you can skip making a directory if the container
#     # already exists, but still create a file in that directory
#     fs.mkdir("new-container/dir/file.txt", exists_ok=True)
#     assert "new-container/" in fs.ls("")

#     fs.mkdir("new-container/file2.txt", exists_ok=True)
#     with fs.open("new-container/file2.txt", "wb") as f:
#         f.write(b"0123456789")
#     assert "new-container/file2.txt" in fs.ls("new-container")

#     fs.mkdir("new-container/dir/file2.txt", exists_ok=True)
#     with fs.open("new-container/dir/file2.txt", "wb") as f:
#         f.write(b"0123456789")
#     assert "new-container/dir/file2.txt" in fs.ls("new-container/dir")

#     # Also verify you can make a nested directory structure
#     fs.mkdir("new-container/dir2/file.txt", exists_ok=True)
#     with fs.open("new-container/dir2/file.txt", "wb") as f:
#         f.write(b"0123456789")
#     assert "new-container/dir2/file.txt" in fs.ls("new-container/dir2")
#     fs.rm("new-container/dir2", recursive=True)

#     fs.rm("new-container/dir", recursive=True)
#     assert fs.ls("new-container") == [
#         "new-container/file.txt",
#         "new-container/file2.txt",
#     ]

#     fs.rm("new-container/file.txt")
#     fs.rm("new-container/file2.txt")
#     fs.rmdir("new-container")

#     assert "new-container/" not in fs.ls("")


# def test_large_blob(storage):
#     import tempfile
#     import hashlib
#     import io
#     import shutil
#     from pathlib import Path

#     fs = adlfs.AzureBlobFileSystem(
#         storage.account_name,
#         storage.account_key,
#         custom_domain=f"http://{storage.primary_endpoint}",
#     )

#     # create a 20MB byte array, ensure it's larger than blocksizes to force a
#     # chuncked upload
#     blob_size = 20_000_000
#     assert blob_size > fs.blocksize
#     assert blob_size > adlfs.AzureBlobFile.DEFAULT_BLOCK_SIZE

#     data = b"1" * blob_size
#     _hash = hashlib.md5(data)
#     expected = _hash.hexdigest()

#     # create container
#     fs.mkdir("chunk-container")

#     # upload the data using fs.open
#     path = "chunk-container/large-blob.bin"
#     with fs.open(path, "wb") as dst:
#         dst.write(data)

#     assert fs.exists(path)
#     assert fs.size(path) == blob_size

#     del data

#     # download with fs.open
#     bio = io.BytesIO()
#     with fs.open(path, "rb") as src:
#         shutil.copyfileobj(src, bio)

#     # read back the data and calculate md5
#     bio.seek(0)
#     data = bio.read()
#     _hash = hashlib.md5(data)
#     result = _hash.hexdigest()

#     assert expected == result

#     # do the same but using upload/download and a tempdir
#     path = path = "chunk-container/large_blob2.bin"
#     with tempfile.TemporaryDirectory() as td:
#         local_blob: Path = Path(td) / "large_blob2.bin"
#         with local_blob.open("wb") as fo:
#             fo.write(data)
#         assert local_blob.exists()
#         assert local_blob.stat().st_size == blob_size

#         fs.upload(str(local_blob), path)
#         assert fs.exists(path)
#         assert fs.size(path) == blob_size

#         # download now
#         local_blob.unlink()
#         fs.download(path, str(local_blob))
#         assert local_blob.exists()
#         assert local_blob.stat().st_size == blob_size
