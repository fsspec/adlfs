import os
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError

from adlfs import OneLakeFile, OneLakeFileSystem


def create_async_context_manager_mock():
    """Helper to create a proper async context manager mock."""
    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_context)
    mock_context.__aexit__ = AsyncMock(return_value=False)
    return mock_context


class TestOneLakeFileSystem:
    """Test cases for OneLakeFileSystem"""

    def test_init_with_credentials(self):
        """Test initialization with client credentials"""
        fs = OneLakeFileSystem(
            account_name="onelake",
            workspace_name="test_workspace",
            lakehouse_name="test_lakehouse",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )

        assert fs.account_name == "onelake"
        assert fs.workspace_name == "test_workspace"
        assert fs.lakehouse_name == "test_lakehouse"
        assert fs.tenant_id == "test-tenant"
        assert fs.client_id == "test-client"
        assert fs.client_secret == "test-secret"
        assert not fs.anon
        assert fs.account_url == "https://onelake.dfs.fabric.microsoft.com"

    def test_init_anonymous(self):
        """Test initialization with anonymous access"""
        fs = OneLakeFileSystem(anon=True)

        assert fs.account_name == "onelake"
        assert fs.anon is True

    def test_init_with_env_vars(self):
        """Test initialization using environment variables"""
        with mock.patch.dict(
            os.environ,
            {
                "AZURE_TENANT_ID": "env-tenant",
                "AZURE_CLIENT_ID": "env-client",
                "AZURE_CLIENT_SECRET": "env-secret",
            },
        ):
            fs = OneLakeFileSystem()

            assert fs.tenant_id == "env-tenant"
            assert fs.client_id == "env-client"
            assert fs.client_secret == "env-secret"

    def test_strip_protocol(self):
        """Test URL protocol stripping"""
        assert (
            OneLakeFileSystem._strip_protocol("onelake://workspace/lakehouse/file.txt")
            == "workspace/lakehouse/file.txt"
        )
        assert (
            OneLakeFileSystem._strip_protocol("workspace/lakehouse/file.txt")
            == "workspace/lakehouse/file.txt"
        )
        assert (
            OneLakeFileSystem._strip_protocol("/workspace/lakehouse/file.txt")
            == "workspace/lakehouse/file.txt"
        )

        # Test abfss URL stripping for OneLake
        assert (
            OneLakeFileSystem._strip_protocol(
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/file.txt"
            )
            == "workspace/lakehouse/Files/file.txt"
        )

    def test_get_kwargs_from_urls(self):
        """Test URL parsing for kwargs extraction"""
        kwargs = OneLakeFileSystem._get_kwargs_from_urls(
            "onelake://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/file.txt"
        )

        assert kwargs.get("account_name") == "onelake"
        assert kwargs.get("workspace_name") == "workspace"
        assert kwargs.get("lakehouse_name") == "lakehouse"

        # Test abfss URL parsing for OneLake
        abfss_kwargs = OneLakeFileSystem._get_kwargs_from_urls(
            "abfss://q_dev_workspace@onelake.dfs.fabric.microsoft.com/qdata_dev_lh.Lakehouse/Files/Upload_Test"
        )

        assert abfss_kwargs.get("account_name") == "onelake"
        assert abfss_kwargs.get("workspace_name") == "q_dev_workspace"
        assert abfss_kwargs.get("lakehouse_name") == "qdata_dev_lh.Lakehouse"

    def test_split_path(self):
        """Test path splitting into components"""
        fs = OneLakeFileSystem(anon=True)

        # Test full path
        workspace, lakehouse, file_path = fs.split_path(
            "workspace/lakehouse/folder/file.txt"
        )
        assert workspace == "workspace"
        assert lakehouse == "lakehouse"
        assert file_path == "folder/file.txt"

        # Test partial paths
        workspace, lakehouse, file_path = fs.split_path("workspace/lakehouse")
        assert workspace == "workspace"
        assert lakehouse == "lakehouse"
        assert file_path == ""

        workspace, lakehouse, file_path = fs.split_path("workspace")
        assert workspace == "workspace"
        assert lakehouse == ""
        assert file_path == ""

        # Test empty path
        workspace, lakehouse, file_path = fs.split_path("")
        assert workspace == ""
        assert lakehouse == ""
        assert file_path == ""

    @pytest.mark.asyncio
    async def test_ls_with_workspace_and_lakehouse(self):
        """Test listing files when workspace and lakehouse are known"""
        with patch("adlfs.onelake.AIODataLakeServiceClient"):
            fs = OneLakeFileSystem(
                workspace_name="test_workspace",
                lakehouse_name="test_lakehouse",
                anon=True,
            )

            # Mock the service client - don't make it an AsyncMock since we need regular return values
            mock_service_client = MagicMock()
            fs.service_client = mock_service_client

            # Create the file system client as an async context manager
            mock_file_system_client = (
                MagicMock()
            )  # Changed from AsyncMock to MagicMock for non-async methods
            mock_fs_context = create_async_context_manager_mock()
            mock_fs_context.__aenter__.return_value = mock_file_system_client
            mock_service_client.get_file_system_client.return_value = mock_fs_context

            # Mock path items
            mock_path1 = MagicMock()
            mock_path1.name = "file1.txt"
            mock_path1.is_directory = False
            mock_path1.content_length = 1024

            mock_path2 = MagicMock()
            mock_path2.name = "folder1"
            mock_path2.is_directory = True

            class MockAsyncIterator:
                def __init__(self, items):
                    self.items = items
                    self.index = 0

                def __aiter__(self):
                    return self

                async def __anext__(self):
                    if self.index >= len(self.items):
                        raise StopAsyncIteration
                    item = self.items[self.index]
                    self.index += 1
                    return item

            mock_file_system_client.get_paths.return_value = MockAsyncIterator(
                [mock_path1, mock_path2]
            )

            result = await fs._ls("test_workspace/test_lakehouse/")

            assert len(result) == 2
            assert result[0]["name"] == "test_workspace/test_lakehouse/file1.txt"
            assert result[0]["type"] == "file"
            assert result[0]["size"] == 1024
            assert result[1]["name"] == "test_workspace/test_lakehouse/folder1"
            assert result[1]["type"] == "directory"

    @pytest.mark.asyncio
    async def test_ls_file_not_found(self):
        """Test listing when path doesn't exist"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_paths.side_effect = ResourceNotFoundError(
            "Path not found"
        )

        fs.service_client = mock_service_client

        with pytest.raises(FileNotFoundError):
            await fs._ls("workspace/lakehouse/nonexistent")

    @pytest.mark.asyncio
    async def test_info_file(self):
        """Test getting file information"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client

        mock_properties = MagicMock()
        mock_properties.size = 2048
        mock_properties.last_modified = "2023-01-01T00:00:00Z"

        mock_file_client.get_file_properties.return_value = mock_properties
        fs.service_client = mock_service_client

        result = await fs._info("workspace/lakehouse/file.txt")

        assert result["name"] == "workspace/lakehouse/file.txt"
        assert result["type"] == "file"
        assert result["size"] == 2048

    @pytest.mark.asyncio
    async def test_info_directory(self):
        """Test getting directory information"""
        fs = OneLakeFileSystem(anon=True)

        result = await fs._info("workspace/lakehouse")

        assert result["name"] == "workspace/lakehouse"
        assert result["type"] == "directory"
        assert result["size"] is None

    @pytest.mark.asyncio
    async def test_cat_file(self):
        """Test reading file content"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()
        mock_download_stream = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client
        mock_file_client.download_file.return_value = mock_download_stream
        mock_download_stream.readall.return_value = b"file content"

        fs.service_client = mock_service_client

        result = await fs._cat_file("workspace/lakehouse/file.txt")

        assert result == b"file content"
        mock_file_client.download_file.assert_called_once_with(offset=None, length=None)

    @pytest.mark.asyncio
    async def test_cat_file_with_range(self):
        """Test reading file content with byte range"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()
        mock_download_stream = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client
        mock_file_client.download_file.return_value = mock_download_stream
        mock_download_stream.readall.return_value = b"content"

        fs.service_client = mock_service_client

        result = await fs._cat_file("workspace/lakehouse/file.txt", start=10, end=20)

        assert result == b"content"
        mock_file_client.download_file.assert_called_once_with(offset=10, length=10)

    @pytest.mark.asyncio
    async def test_cat_file_not_found(self):
        """Test reading non-existent file"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client
        mock_file_client.download_file.side_effect = ResourceNotFoundError(
            "File not found"
        )

        fs.service_client = mock_service_client

        with pytest.raises(FileNotFoundError):
            await fs._cat_file("workspace/lakehouse/nonexistent.txt")

    @pytest.mark.asyncio
    async def test_pipe_file(self):
        """Test writing file content"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client

        fs.service_client = mock_service_client

        test_data = b"test data"
        await fs._pipe_file("workspace/lakehouse/newfile.txt", test_data)

        mock_file_client.create_file.assert_called_once()
        mock_file_client.append_data.assert_called_once_with(
            test_data, offset=0, length=len(test_data)
        )
        mock_file_client.flush_data.assert_called_once_with(len(test_data))

    @pytest.mark.asyncio
    async def test_rm_file(self):
        """Test deleting a file"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client

        fs.service_client = mock_service_client

        await fs._rm_file("workspace/lakehouse/file.txt")

        mock_file_client.delete_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_rm_file_not_found(self):
        """Test deleting non-existent file (should not raise)"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_file_client = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_file_client.return_value = mock_file_client
        mock_file_client.delete_file.side_effect = ResourceNotFoundError(
            "File not found"
        )

        fs.service_client = mock_service_client

        # Should not raise an exception
        await fs._rm_file("workspace/lakehouse/nonexistent.txt")

    @pytest.mark.asyncio
    async def test_mkdir(self):
        """Test creating a directory"""
        fs = OneLakeFileSystem(anon=True)

        mock_service_client = AsyncMock()
        mock_file_system_client = AsyncMock()
        mock_directory_client = AsyncMock()

        mock_service_client.get_file_system_client.return_value = (
            mock_file_system_client
        )
        mock_file_system_client.get_directory_client.return_value = (
            mock_directory_client
        )

        fs.service_client = mock_service_client

        await fs._mkdir("workspace/lakehouse/newfolder")

        mock_directory_client.create_directory.assert_called_once()

    def test_mkdir_invalid_path(self):
        """Test creating directory with invalid path"""
        fs = OneLakeFileSystem(anon=True)

        with pytest.raises(NotImplementedError):
            fs.mkdir("workspace")  # Can't create workspace

    def test_invalid_path_formats(self):
        """Test handling of invalid path formats"""
        fs = OneLakeFileSystem(anon=True)

        # Test invalid paths for file operations
        with pytest.raises(ValueError):
            OneLakeFile(fs, "invalid_path", mode="rb")

        with pytest.raises(ValueError):
            OneLakeFile(fs, "workspace", mode="rb")  # Missing lakehouse and file

        with pytest.raises(ValueError):
            OneLakeFile(fs, "workspace/lakehouse", mode="rb")  # Missing file


class TestOneLakeFile:
    """Test cases for OneLakeFile"""

    def test_init_valid_path(self):
        """Test file initialization with valid path"""
        fs = OneLakeFileSystem(anon=True)

        file_obj = OneLakeFile(fs, "workspace/lakehouse/file.txt", mode="rb")

        assert file_obj.workspace == "workspace"
        assert file_obj.lakehouse == "lakehouse"
        assert file_obj.file_path == "file.txt"
        assert file_obj.container_path == "workspace/lakehouse"

    def test_init_invalid_path(self):
        """Test file initialization with invalid path"""
        fs = OneLakeFileSystem(anon=True)

        with pytest.raises(ValueError):
            OneLakeFile(fs, "invalid", mode="rb")

    @patch("adlfs.onelake.sync")
    def test_fetch_range(self, mock_sync):
        """Test fetching byte range from file"""
        fs = OneLakeFileSystem(anon=True)
        file_obj = OneLakeFile(fs, "workspace/lakehouse/file.txt", mode="rb")

        mock_sync.return_value = b"test data"

        result = file_obj._fetch_range(0, 10)

        assert result == b"test data"
        mock_sync.assert_called_once()

    @patch("adlfs.onelake.sync")
    def test_upload_chunk(self, mock_sync):
        """Test uploading chunk of data"""
        fs = OneLakeFileSystem(anon=True)
        file_obj = OneLakeFile(fs, "workspace/lakehouse/file.txt", mode="wb")

        # Mock the buffer
        file_obj.buffer = MagicMock()
        file_obj.buffer.getvalue.return_value = b"test data"
        file_obj.offset = 0

        result = file_obj._upload_chunk(final=True)

        assert result is True
        mock_sync.assert_called_once()

    def test_protocols(self):
        """Test that OneLake protocols are registered"""
        assert "onelake" in OneLakeFileSystem.protocol
        assert "abfss" in OneLakeFileSystem.protocol


class TestOneLakeURLRouting:
    """Test URL routing between AzureBlobFileSystem and OneLakeFileSystem"""

    def test_onelake_url_routing(self):
        """Test that OneLake URLs are properly parsed and routed."""
        # OneLake URLs should be handled by OneLakeFileSystem
        onelake_url = (
            "abfss://q_dev_workspace@onelake.dfs.fabric.microsoft.com/"
            "qdata_dev_lh.Lakehouse/Files/Upload_Test"
        )

        # Test OneLakeFileSystem can handle both protocols
        assert "abfss" in OneLakeFileSystem.protocol
        assert "onelake" in OneLakeFileSystem.protocol

        # Test URL parsing for OneLake
        kwargs = OneLakeFileSystem._get_kwargs_from_urls(onelake_url)
        assert kwargs.get("account_name") == "onelake"
        assert kwargs.get("workspace_name") == "q_dev_workspace"
        assert kwargs.get("lakehouse_name") == "qdata_dev_lh.Lakehouse"

        # Test path stripping for OneLake URLs
        stripped = OneLakeFileSystem._strip_protocol(onelake_url)
        assert stripped == "q_dev_workspace/qdata_dev_lh.Lakehouse/Files/Upload_Test"

    def test_azure_blob_url_routing(self):
        """Test that regular Azure Storage URLs are handled by AzureBlobFileSystem."""
        from adlfs import AzureBlobFileSystem

        # Regular Azure Storage URL
        azure_url = "abfss://container@storageaccount.dfs.core.windows.net/path/to/file"

        # Test that AzureBlobFileSystem doesn't process OneLake URLs
        onelake_url = (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/path"
        )
        onelake_kwargs = AzureBlobFileSystem._get_kwargs_from_urls(onelake_url)
        assert not onelake_kwargs  # Should return empty dict for OneLake URLs

        # Test that AzureBlobFileSystem handles regular Azure URLs
        azure_kwargs = AzureBlobFileSystem._get_kwargs_from_urls(azure_url)
        assert azure_kwargs.get("account_name") == "storageaccount"

    def test_onelake_strip_protocol_variations(self):
        """Test OneLake URL stripping with different URL formats."""

        test_cases = [
            # (input_url, expected_stripped_path)
            (
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/test.txt",
                "workspace/lakehouse/Files/test.txt",
            ),
            (
                "onelake://workspace/lakehouse/Files/test.txt",
                "workspace/lakehouse/Files/test.txt",
            ),
            (
                "workspace/lakehouse/Files/test.txt",
                "workspace/lakehouse/Files/test.txt",
            ),
        ]

        for url, expected in test_cases:
            result = OneLakeFileSystem._strip_protocol(url)
            assert result == expected, f"Failed for URL: {url}"

    def test_onelake_get_kwargs_variations(self):
        """Test OneLake URL parameter extraction with different formats."""

        test_cases = [
            # abfss format with workspace in host part
            {
                "url": (
                    "abfss://q_dev_workspace@onelake.dfs.fabric.microsoft.com/"
                    "qdata_dev_lh.Lakehouse/Files/Upload_Test"
                ),
                "expected": {
                    "account_name": "onelake",
                    "workspace_name": "q_dev_workspace",
                    "lakehouse_name": "qdata_dev_lh.Lakehouse",
                },
            },
            # onelake format with workspace in path
            {
                "url": (
                    "onelake://onelake.dfs.fabric.microsoft.com/"
                    "workspace/lakehouse/Files/test.txt"
                ),
                "expected": {
                    "account_name": "onelake",
                    "workspace_name": "workspace",
                    "lakehouse_name": "lakehouse",
                },
            },
        ]

        for test_case in test_cases:
            kwargs = OneLakeFileSystem._get_kwargs_from_urls(test_case["url"])
            for key, expected_value in test_case["expected"].items():
                assert (
                    kwargs.get(key) == expected_value
                ), f"Failed for URL: {test_case['url']}, key: {key}, got: {kwargs.get(key)}, expected: {expected_value}"

    def test_azure_blob_ignores_onelake_domains(self):
        """Test that AzureBlobFileSystem ignores OneLake domain URLs."""
        from adlfs import AzureBlobFileSystem

        onelake_urls = [
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/file",
            "abfs://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/file",
        ]

        for url in onelake_urls:
            kwargs = AzureBlobFileSystem._get_kwargs_from_urls(url)
            # Should return empty dict (no account_name extracted)
            assert kwargs == {}, f"AzureBlobFileSystem should ignore OneLake URL: {url}"

    def test_protocol_overlap_handling(self):
        """Test that protocol overlap between filesystems is handled correctly."""
        from adlfs import AzureBlobFileSystem

        # Both filesystems support abfss protocol
        assert "abfss" in AzureBlobFileSystem.protocol
        assert "abfss" in OneLakeFileSystem.protocol

        # But they should handle different domains
        azure_url = "abfss://container@account.dfs.core.windows.net/file"
        onelake_url = (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/file"
        )

        # Azure should handle core.windows.net, ignore fabric.microsoft.com
        azure_kwargs = AzureBlobFileSystem._get_kwargs_from_urls(azure_url)
        onelake_kwargs_from_azure = AzureBlobFileSystem._get_kwargs_from_urls(
            onelake_url
        )

        assert azure_kwargs.get("account_name") == "account"
        assert onelake_kwargs_from_azure == {}  # Should be empty

        # OneLake should handle fabric.microsoft.com URLs
        onelake_kwargs = OneLakeFileSystem._get_kwargs_from_urls(onelake_url)
        assert onelake_kwargs.get("account_name") == "onelake"
        assert onelake_kwargs.get("workspace_name") == "workspace"


class TestOneLakeIntegration:
    """Integration tests for OneLake functionality"""

    def test_fsspec_integration(self):
        """Test that OneLake can be used with fsspec.open"""
        import fsspec

        # Register the protocol
        fsspec.register_implementation("onelake", OneLakeFileSystem)

        # Test that the protocol is registered
        assert "onelake" in fsspec.available_protocols()

        # Test URL parsing
        with mock.patch("adlfs.onelake.OneLakeFileSystem.do_connect"):
            fs = fsspec.filesystem("onelake", anon=True)
            assert isinstance(fs, OneLakeFileSystem)

    def test_sync_methods(self):
        """Test that sync wrapper methods work"""
        fs = OneLakeFileSystem(anon=True)

        # These should be callable (though they might raise without proper mocking)
        assert hasattr(fs, "ls")
        assert hasattr(fs, "info")
        assert hasattr(fs, "cat_file")
        assert hasattr(fs, "pipe_file")
        assert hasattr(fs, "rm_file")
        assert hasattr(fs, "mkdir")
        assert callable(fs.ls)
        assert callable(fs.info)
        assert callable(fs.cat_file)
        assert callable(fs.pipe_file)
        assert callable(fs.rm_file)
        assert callable(fs.mkdir)
