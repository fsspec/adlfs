import adlfs
import docker
import pytest


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
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )


def test_ls(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )

    ## these are containers
    assert fs.ls("") == ["data/"]
    assert fs.ls("/") == ["data/"]

    ## these are top-level directories and files
    assert fs.ls("data") == ["data/root/", "data/top_file.txt"]
    assert fs.ls("/data") == ["data/root/", "data/top_file.txt"]

    ## root contains files and directories
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

    ## if not direct match is found throws error
    with pytest.raises(FileNotFoundError):
        fs.ls("not-a-container")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/not-a-directory/")

    with pytest.raises(FileNotFoundError):
        fs.ls("data/root/not-a-file.txt")


def test_info(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )

    container_info = fs.info("data")
    assert container_info == {"name": "data/", "type": "directory", "size": 0}

    dir_info = fs.info("data/root/c")
    assert dir_info == {"name": "data/root/c/", "type": "directory", "size": 0}
    file_info = fs.info("data/root/a/file.txt")
    assert file_info == {"name": "data/root/a/file.txt", "type": "file", "size": 10}


def test_glob(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )

    ## just the directory name
    assert fs.glob("data/root") == ["data/root"]
    ## top-level contents of a directory
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

    assert fs.glob("data/root/b/*") == ["data/root/b/file.txt"]

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

    ## missing
    assert fs.glob("data/missing/*") == []


def test_open_file(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )
    f = fs.open("/data/root/a/file.txt")

    result = f.read()
    assert result == b"0123456789"


def test_rm(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )

    fs.rm("/data/root/a/file.txt")

    with pytest.raises(FileNotFoundError):
        fs.ls("/data/root/a/file.txt")


def test_mkdir_rmdir(storage):
    fs = adlfs.AzureBlobFileSystem(
        storage.account_name,
        storage.account_key,
        custom_domain=f"http://{storage.primary_endpoint}",
    )

    fs.mkdir("new-container")
    assert "new-container/" in fs.ls("")

    with fs.open("new-container/file.txt", "wb") as f:
        f.write(b"0123456789")

    with fs.open("new-container/dir/file.txt", "wb") as f:
        f.write(b"0123456789")
    with fs.open("new-container/dir/file.txt", "wb") as f:
        f.write(b"0123456789")

    fs.rm("new-container/dir", recursive=True)
    assert fs.ls("new-container") == ["new-container/file.txt"]

    fs.rm("new-container/file.txt")
    fs.rmdir("new-container")

    assert "new-container/" not in fs.ls("")
