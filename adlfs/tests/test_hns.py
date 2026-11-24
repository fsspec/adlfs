"""
Tests for Hierarchical Namespace (HNS) features.

These tests require a real ADLS Gen2 storage account (Azurite does not
support HNS) and are fully self-contained — no dependency on conftest.py.

Environment variables:
    ADLFS_TEST_ACCOUNT_NAME    Storage account name (required)
    ADLFS_TEST_CONTAINER       Container name (default: adlfs-hns-test)
    ADLFS_TEST_ACCOUNT_KEY     Shared key (optional; omit for managed identity)
    ADLFS_TEST_ACCOUNT_HOST    Custom endpoint host (optional)
    AZURE_CLIENT_ID            UAMI client ID (optional; for user-assigned MI)

Authentication precedence:
    account_key provided → shared-key auth
    No account_key → DefaultAzureCredential (picks up managed identity,
                       environment variables, AZ CLI login, etc.)

Run:
    AZURE_CLIENT_ID=<id> ADLFS_TEST_ACCOUNT_NAME=myaccount pytest tests/test_hns.py -v
"""

import os

import pytest

from adlfs import AzureBlobFileSystem


def _config():
    """Return (account_name, account_key, container, account_host) from env."""
    name = os.environ["ADLFS_TEST_ACCOUNT_NAME"]
    key = os.environ.get("ADLFS_TEST_ACCOUNT_KEY", "")
    container = os.environ.get("ADLFS_TEST_CONTAINER", "adlfs-hns-test")
    host = os.environ.get("ADLFS_TEST_ACCOUNT_HOST", "")
    return name, key, container, host


def _require_live_account():
    """Skip if ADLS Gen2 account name is not set."""
    if "ADLFS_TEST_ACCOUNT_NAME" not in os.environ:
        pytest.skip("Set ADLFS_TEST_ACCOUNT_NAME to run HNS integration tests")


def _make_fs(account_name, account_key, account_host, **extra):
    """Build AzureBlobFileSystem with the right auth for the test environment."""
    kw = {}
    if account_host:
        kw["account_host"] = account_host
    if account_key:
        kw["account_key"] = account_key
    kw.update(extra)
    return AzureBlobFileSystem(account_name=account_name, **kw)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def spawn_azurite():
    """HNS integration tests run against real ADLS Gen2; bypass Azurite container."""
    yield None


@pytest.fixture(scope="session")
def hns_fs():
    """AzureBlobFileSystem connected via auto-detection (DFS-first with fallback)."""
    _require_live_account()
    name, key, container, host = _config()
    fs = _make_fs(name, key, host)
    if container not in [c["name"] for c in fs.ls("", detail=True)]:
        fs.mkdir(container)
    yield fs
    try:
        for item in fs.ls(container):
            fs.rm(item, recursive=True)
    except Exception:
        pass


@pytest.fixture
def hns_fs_blob_uri():
    """Force Blob endpoint via a blob.core.windows.net URI."""
    _require_live_account()
    name, key, _, host = _config()
    kw = {}
    if key:
        kw["account_key"] = key
    uri = f"abfss://none@{name}.blob.core.windows.net/none"
    fs = AzureBlobFileSystem(uri, **kw)
    yield fs


@pytest.fixture
def hns_fs_account_host():
    """Force DFS endpoint via explicit account_host."""
    _require_live_account()
    name, key, _, _ = _config()
    kw = {}
    if key:
        kw["account_key"] = key
    fs = AzureBlobFileSystem(
        account_name=name,
        account_host=f"{name}.dfs.core.windows.net",
        **kw,
    )
    yield fs


# ---------------------------------------------------------------------------
# Phase 1 — Endpoint selection and HNS detection
# ---------------------------------------------------------------------------


class TestEndpointSelection:
    """Verify the do_connect() precedence chain."""

    def test_hns_detected_by_default(self, hns_fs):
        """Default auto-detect sets hns_enabled=True for Gen2 accounts."""
        assert hns_fs.hns_enabled is False
        # account_url remains the blob endpoint; DFS URL stored separately
        assert ".blob.core." in hns_fs.account_url
        assert hasattr(hns_fs, "_dfs_url")

    def test_blob_uri_skips_hns_probe(self, hns_fs_blob_uri):
        """URI with blob.core.windows.net uses Blob endpoint directly, no probe."""
        assert hns_fs_blob_uri.hns_enabled is False
        assert ".blob.core." in hns_fs_blob_uri.account_url

    def test_account_host_wins(self, hns_fs_account_host):
        """account_host takes absolute precedence for endpoint URL."""
        # HNS is still detected via SDK, even with custom account_host
        assert hns_fs_account_host.hns_enabled is True
        assert ".dfs.core." in hns_fs_account_host.account_url


# ---------------------------------------------------------------------------
# Phase 2 — HNS-aware _dir_exists
# ---------------------------------------------------------------------------


class TestDirExists:
    """Verify optimized directory-existence checks on HNS."""

    def test_dir_exists_for_existing_directory(self, hns_fs):
        name, _, container, _ = _config()
        dir_path = f"{container}/subdir"
        hns_fs.pipe(f"{dir_path}/afile.txt", b"hello")
        assert hns_fs.exists(dir_path)
        assert hns_fs.isdir(dir_path)

    def test_dir_exists_for_nonexistent_directory(self, hns_fs):
        name, _, container, _ = _config()
        assert not hns_fs.exists(f"{container}/nonexistent")

    def test_file_is_not_dir(self, hns_fs):
        name, _, container, _ = _config()
        path = f"{container}/plain_file.txt"
        hns_fs.pipe(path, b"hello")
        assert not hns_fs.isdir(path)
        assert hns_fs.isfile(path)

    def test_nested_directory_hierarchy(self, hns_fs):
        name, _, container, _ = _config()
        deep = f"{container}/a/b/c"
        hns_fs.pipe(f"{deep}/file.txt", b"nested")
        assert hns_fs.isdir(f"{container}/a")
        assert hns_fs.isdir(f"{container}/a/b")
        assert hns_fs.isdir(deep)
        assert hns_fs.isfile(f"{deep}/file.txt")


# ---------------------------------------------------------------------------
# Phase 3 — DFS-native rename
# ---------------------------------------------------------------------------


class TestRename:
    """Verify _mv_file behavior for both same-container and cross-container."""

    def test_rename_same_container(self, hns_fs):
        name, _, container, _ = _config()
        src = f"{container}/source.txt"
        dst = f"{container}/target.txt"
        content = b"rename-me"

        hns_fs.pipe(src, content)
        hns_fs.mv(src, dst)

        assert not hns_fs.exists(src)
        assert hns_fs.exists(dst)
        assert hns_fs.cat(dst) == content

    def test_rename_preserves_metadata(self, hns_fs):
        name, _, container, _ = _config()
        src = f"{container}/meta_src.txt"
        dst = f"{container}/meta_dst.txt"
        content = b"metadata-test"

        hns_fs.pipe(src, content)
        src_info = hns_fs.info(src)
        hns_fs.mv(src, dst)
        dst_info = hns_fs.info(dst)

        assert dst_info["size"] == src_info["size"]
        assert hns_fs.cat(dst) == content

    def test_rename_cross_container(self, hns_fs):
        name, _, container1, _ = _config()
        container2 = f"{container1}-cross"
        hns_fs.mkdir(container2)

        try:
            src = f"{container1}/cross_src.txt"
            dst = f"{container2}/cross_dst.txt"
            content = b"cross-container"

            hns_fs.pipe(src, content)
            hns_fs.mv(src, dst)

            assert not hns_fs.exists(src)
            assert hns_fs.exists(dst)
            assert hns_fs.cat(dst) == content
        finally:
            hns_fs.rm(container2, recursive=True)

    def test_rename_same_path_noop(self, hns_fs):
        name, _, container, _ = _config()
        path = f"{container}/noop.txt"
        hns_fs.pipe(path, b"noop")
        hns_fs.mv(path, path)
        assert hns_fs.exists(path)
        assert hns_fs.cat(path) == b"noop"

    def test_rename_directory(self, hns_fs):
        name, key, container, _ = _config()
        fs = _make_fs(f"{name}.dfs.core.windows.net", key, "")
        src_dir = f"{container}/srcdir"
        dst_dir = f"{container}/dstdir"
        fs.pipe(f"{src_dir}/a.txt", b"a")
        fs.pipe(f"{src_dir}/b.txt", b"b")

        fs.mv(src_dir, dst_dir, recursive=True)

        assert not fs.exists(src_dir)
        assert fs.isdir(dst_dir)
        assert fs.cat(f"{dst_dir}/a.txt") == b"a"
        assert fs.cat(f"{dst_dir}/b.txt") == b"b"


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------


class TestIntegration:
    """End-to-end workflow on HNS."""

    def test_full_workflow(self, hns_fs):
        """Create, list, read, rename, delete — end to end."""
        name, _, container, _ = _config()

        prefix = "wf"
        hns_fs.pipe(f"{container}/{prefix}/a.txt", b"aaa")
        hns_fs.pipe(f"{container}/{prefix}/subdir/b.txt", b"bbb")

        listing = hns_fs.ls(f"{container}/{prefix}", detail=True)
        names = {e["name"] for e in listing}
        assert f"{container}/{prefix}/a.txt" in names
        assert f"{container}/{prefix}/subdir" in names

        sub = hns_fs.ls(f"{container}/{prefix}/subdir")
        assert sub == [f"{container}/{prefix}/subdir/b.txt"]

        hns_fs.mv(f"{container}/{prefix}/a.txt", f"{container}/{prefix}/renamed.txt")
        assert not hns_fs.exists(f"{container}/{prefix}/a.txt")
        assert hns_fs.cat(f"{container}/{prefix}/renamed.txt") == b"aaa"

        hns_fs.rm(f"{container}/{prefix}/subdir", recursive=True)
        assert not hns_fs.exists(f"{container}/{prefix}/subdir")

    def test_find_recursive(self, hns_fs):
        name, _, container, _ = _config()
        hns_fs.pipe(f"{container}/d1/f1.txt", b"1")
        hns_fs.pipe(f"{container}/d1/d2/f2.txt", b"2")

        found = hns_fs.find(container)
        assert f"{container}/d1/f1.txt" in found
        assert f"{container}/d1/d2/f2.txt" in found

    def test_open_read_write(self, hns_fs):
        name, _, container, _ = _config()
        path = f"{container}/readwrite.txt"
        data = b"hello-hns"

        with hns_fs.open(path, "wb") as f:
            f.write(data)

        with hns_fs.open(path, "rb") as f:
            assert f.read() == data

    def test_cp_file_within_container(self, hns_fs):
        name, _, container, _ = _config()
        src = f"{container}/cp_src.txt"
        dst = f"{container}/cp_dst.txt"
        content = b"copy-me"

        hns_fs.pipe(src, content)
        hns_fs.cp(src, dst)

        assert hns_fs.exists(src)
        assert hns_fs.exists(dst)
        assert hns_fs.cat(src) == hns_fs.cat(dst)
