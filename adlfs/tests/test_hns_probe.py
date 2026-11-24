"""Unit tests for DFS endpoint probing and initialization behavior."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from azure.core.exceptions import HttpResponseError, ServiceRequestError

from adlfs import AzureBlobFileSystem


@pytest.fixture(autouse=True)
def spawn_azurite():
    """HNS unit tests do not require Azurite; bypass conftest.py fixture."""
    yield None


@pytest.mark.asyncio
async def test_check_dfs_reachability_success():
    """Verify reachability returns True when server responds (even with HTTP error status)."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    
    with patch("azure.storage.filedatalake.aio.DataLakeServiceClient") as mock_cls:
        mock_client = MagicMock()
        mock_client._pipeline.run = AsyncMock(side_effect=HttpResponseError(message="Forbidden"))
        mock_client.close = AsyncMock()
        mock_cls.return_value = mock_client
        
        reachable = await AzureBlobFileSystem._check_dfs_reachability(
            fs, "https://myaccount.dfs.core.windows.net", timeout=1.0
        )
        assert reachable is True
        mock_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_dfs_reachability_network_failure():
    """Verify reachability returns False on network timeout / drop."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    
    with patch("azure.storage.filedatalake.aio.DataLakeServiceClient") as mock_cls:
        mock_client = MagicMock()
        mock_client._pipeline.run = AsyncMock(side_effect=ServiceRequestError("Connection timed out"))
        mock_client.close = AsyncMock()
        mock_cls.return_value = mock_client
        
        reachable = await AzureBlobFileSystem._check_dfs_reachability(
            fs, "https://myaccount.dfs.core.windows.net", timeout=1.0
        )
        assert reachable is False
        mock_client.close.assert_awaited_once()


def test_init_datalake_client_probes_and_disables_on_failure():
    """If probe fails at init, datalake_service_client is set to None."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.hns_enabled = True
    fs._dfs_url = "https://myaccount.dfs.core.windows.net"
    fs.loop = MagicMock()

    with patch("adlfs.spec.sync", return_value=False):
        AzureBlobFileSystem._init_datalake_client(fs)

    assert fs.datalake_service_client is None


def test_init_datalake_client_probes_and_enables_on_success():
    """If probe succeeds at init, datalake_service_client is initialized."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.hns_enabled = True
    fs._dfs_url = "https://myaccount.dfs.core.windows.net"
    fs.credential = MagicMock()
    fs.account_key = None
    fs.sas_token = None
    fs._timeout_kwargs = {"connection_timeout": 10}
    fs.loop = MagicMock()

    with patch("adlfs.spec.sync", return_value=True):
        with patch("adlfs.spec.weakref"):
            with patch("azure.storage.filedatalake.aio.DataLakeServiceClient") as mock_cls:
                AzureBlobFileSystem._init_datalake_client(fs)
                mock_cls.assert_called_once()
                call_kw = mock_cls.call_args.kwargs
                assert call_kw["account_url"] == fs._dfs_url
                assert call_kw["connection_timeout"] == 10
                assert hasattr(fs, "datalake_service_client")


def test_plain_account_name_skips_dfs_probe():
    """Plain account_name without DFS URL pattern skips probe and disables HNS."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.connection_string = None
    fs.account_name = "myaccount"
    fs.kwargs = {}
    fs.account_key = "dummykey"
    fs.credential = None
    fs.sas_token = None
    fs.loop = MagicMock()

    AzureBlobFileSystem.do_connect(fs)

    fs._connect_to.assert_called_once_with("https://myaccount.blob.core.windows.net")
    fs._detect_hns.assert_not_called()
    assert fs._hns_enabled is False
    assert fs._dfs_url is None
    assert fs.account_url == "https://myaccount.blob.core.windows.net"


def test_devstoreaccount1_skips_dfs_probe():
    """Azurite account_name skips DFS probe and disables HNS by virtue of not being a DFS URL."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.connection_string = None
    fs.account_name = "devstoreaccount1"
    fs.kwargs = {}
    fs.account_key = "dummykey"
    fs.credential = None
    fs.sas_token = None
    fs.loop = MagicMock()

    AzureBlobFileSystem.do_connect(fs)

    fs._connect_to.assert_called_once_with("https://devstoreaccount1.blob.core.windows.net")
    fs._detect_hns.assert_not_called()
    assert fs._hns_enabled is False
    assert fs._dfs_url is None


def test_dfs_url_triggers_dfs_probe():
    """URL fitting a DFS endpoint pattern triggers probe and enables HNS."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.connection_string = None
    fs.account_name = "myaccount"
    fs.kwargs = {
        "_url_endpoint_type": "dfs",
        "_url_cloud_suffix": "core.windows.net",
    }
    fs.account_key = "dummykey"
    fs.credential = None
    fs.sas_token = None
    fs.loop = MagicMock()

    with patch("adlfs.spec.sync", return_value=True):
        AzureBlobFileSystem.do_connect(fs)

    fs._connect_to.assert_called_once_with("https://myaccount.blob.core.windows.net")
    assert fs._hns_enabled is True
    assert fs._dfs_url == "https://myaccount.dfs.core.windows.net"


def test_dfs_account_host_triggers_dfs_probe():
    """account_host fitting a DFS endpoint pattern triggers probe."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.connection_string = None
    fs.account_name = "myaccount"
    fs.account_host = "myaccount.dfs.core.windows.net"
    fs.kwargs = {}
    fs.account_key = "dummykey"
    fs.credential = None
    fs.sas_token = None
    fs.loop = MagicMock()

    with patch("adlfs.spec.sync", return_value=True):
        AzureBlobFileSystem.do_connect(fs)

    fs._connect_to.assert_called_once_with("https://myaccount.dfs.core.windows.net")
    assert fs._hns_enabled is True
    assert fs._dfs_url == "https://myaccount.dfs.core.windows.net"


@pytest.mark.asyncio
async def test_mv_file_circuit_breaker():
    """If DFS rename fails, datalake_service_client is disabled (circuit breaker)."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.hns_enabled = True
    fs.datalake_service_client = MagicMock()
    fs._isdir = AsyncMock(return_value=False)
    fs._cp_file = AsyncMock()
    fs._rm_file = AsyncMock()
    fs.split_path = lambda path, **kw: (
        ("container", "src.txt", None) if "src" in path else ("container", "dst.txt", None)
    )

    # Simulate failure on DFS file client rename
    fc = MagicMock()
    fc.rename_file = AsyncMock(side_effect=ServiceRequestError("Firewall dropped connection"))
    fsc = MagicMock()
    fsc.get_file_client = MagicMock(return_value=fc)
    fs.datalake_service_client.get_file_system_client = MagicMock(return_value=fsc)

    await AzureBlobFileSystem._mv_file(fs, "container/src.txt", "container/dst.txt")

    # Circuit breaker tripped:
    assert fs.datalake_service_client is None
    # Fallback copy + delete executed:
    fs._cp_file.assert_awaited_once()
    fs._rm_file.assert_awaited_once()


@pytest.mark.asyncio
async def test_mv_file_skips_dfs_when_client_is_none():
    """When datalake_service_client is None, DFS rename is skipped entirely with 0 delay."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.hns_enabled = True
    fs.datalake_service_client = None
    fs._cp_file = AsyncMock()
    fs._rm_file = AsyncMock()
    fs.split_path = lambda path, **kw: (
        ("container", "src.txt", None) if "src" in path else ("container", "dst.txt", None)
    )

    await AzureBlobFileSystem._mv_file(fs, "container/src.txt", "container/dst.txt")

    # Directly executed copy + delete without touching DFS:
    fs._cp_file.assert_awaited_once()
    fs._rm_file.assert_awaited_once()


def test_blob_url_skips_dfs_probe_and_disables_hns():
    """URLs fitting a blob endpoint pattern skip probe and go directly to blob only."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.connection_string = None
    fs.account_name = "myaccount"
    fs.kwargs = {
        "_url_endpoint_type": "blob",
        "_url_cloud_suffix": "core.windows.net",
    }
    fs.account_key = "dummykey"
    fs.credential = None
    fs.sas_token = None
    fs.loop = MagicMock()

    AzureBlobFileSystem.do_connect(fs)

    fs._connect_to.assert_called_once_with("https://myaccount.blob.core.windows.net")
    fs._detect_hns.assert_not_called()
    assert fs._hns_enabled is False
    assert fs._dfs_url is None
    assert fs.account_url == "https://myaccount.blob.core.windows.net"


def test_onelake_and_sovereign_url_parsing():
    """Verify URL parsing and protocol stripping across OneLake and Sovereign clouds."""
    urls = [
        # Commercial Azure
        (
            "abfss://container@myaccount.blob.core.windows.net/path/file.txt",
            "container/path/file.txt",
            {"account_name": "myaccount", "_url_endpoint_type": "blob", "_url_cloud_suffix": "core.windows.net"},
        ),
        (
            "abfss://container@myaccount.dfs.core.windows.net/path/file.txt",
            "container/path/file.txt",
            {"account_name": "myaccount", "_url_endpoint_type": "dfs", "_url_cloud_suffix": "core.windows.net"},
        ),
        (
            "abfs://myaccount.dfs.core.windows.net/container/path/file.txt",
            "container/path/file.txt",
            {"account_name": "myaccount", "_url_endpoint_type": "dfs", "_url_cloud_suffix": "core.windows.net"},
        ),
        # Sovereign: Azure China
        (
            "abfss://container@myaccount.blob.core.chinacloudapi.cn/path/file.txt",
            "container/path/file.txt",
            {"account_name": "myaccount", "_url_endpoint_type": "blob", "_url_cloud_suffix": "core.chinacloudapi.cn"},
        ),
        (
            "abfs://myaccount.dfs.core.chinacloudapi.cn/container/path/file.txt",
            "container/path/file.txt",
            {"account_name": "myaccount", "_url_endpoint_type": "dfs", "_url_cloud_suffix": "core.chinacloudapi.cn"},
        ),
        # Sovereign: Azure US Gov
        (
            "abfss://container@myaccount.blob.core.usgovcloudapi.net/path/file.txt",
            "container/path/file.txt",
            {"account_name": "myaccount", "_url_endpoint_type": "blob", "_url_cloud_suffix": "core.usgovcloudapi.net"},
        ),
        # OneLake (Microsoft Fabric)
        (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/table",
            "workspace/lakehouse/table",
            {"account_name": "onelake", "_url_endpoint_type": "dfs", "_url_cloud_suffix": "fabric.microsoft.com"},
        ),
        (
            "abfss://workspace@onelake.blob.fabric.microsoft.com/lakehouse/table",
            "workspace/lakehouse/table",
            {"account_name": "onelake", "_url_endpoint_type": "blob", "_url_cloud_suffix": "fabric.microsoft.com"},
        ),
        (
            "abfs://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/table",
            "workspace/lakehouse/table",
            {"account_name": "onelake", "_url_endpoint_type": "dfs", "_url_cloud_suffix": "fabric.microsoft.com"},
        ),
    ]

    for url, expected_path, expected_kwargs in urls:
        stripped = AzureBlobFileSystem._strip_protocol(url)
        assert stripped == expected_path, f"Failed stripping on {url}: got {stripped}"

        kwargs = AzureBlobFileSystem._get_kwargs_from_urls(url)
        assert kwargs == expected_kwargs, f"Failed kwargs on {url}: got {kwargs}"


def test_onelake_blob_bypasses_dfs_probe():
    """OneLake blob URL goes directly to blob-only mode with 0 probe delay."""
    fs = MagicMock(spec=AzureBlobFileSystem)
    fs.connection_string = None
    fs.account_name = "onelake"
    fs.kwargs = {
        "_url_endpoint_type": "blob",
        "_url_cloud_suffix": "fabric.microsoft.com",
    }
    fs.account_key = None
    fs.credential = MagicMock()
    fs.sas_token = None
    fs.loop = MagicMock()

    AzureBlobFileSystem.do_connect(fs)

    fs._connect_to.assert_called_once_with("https://onelake.blob.fabric.microsoft.com")
    fs._detect_hns.assert_not_called()
    assert fs._hns_enabled is False
    assert fs._dfs_url is None
    assert fs.account_url == "https://onelake.blob.fabric.microsoft.com"


