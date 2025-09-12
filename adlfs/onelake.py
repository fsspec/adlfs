# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import logging
import os

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake.aio import (
    DataLakeServiceClient as AIODataLakeServiceClient,
)
from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options

logger = logging.getLogger(__name__)


class OneLakeFileSystem(AsyncFileSystem):
    """
    Access Microsoft OneLake using Azure Data Lake Storage Gen2 API.

    OneLake is the built-in, data lake for Microsoft Fabric that's automatically provisioned
    with every Microsoft Fabric tenant.

    Parameters
    ----------
    account_name : str
        OneLake account name (typically in the format: onelake.dfs.fabric.microsoft.com)
    workspace_name : str, optional
        The name of the Fabric workspace
    lakehouse_name : str, optional
        The name of the lakehouse within the workspace
    tenant_id : str
        Azure tenant ID for authentication
    client_id : str
        Azure AD application client ID
    client_secret : str
        Azure AD application client secret
    credential : azure.core.credentials.TokenCredential, optional
        Azure credential object for authentication
    anon : bool, optional
        Use anonymous access (default: False)

    Examples
    --------
    >>> from adlfs import OneLakeFileSystem
    >>> fs = OneLakeFileSystem(
    ...     account_name="onelake",
    ...     tenant_id="your-tenant-id",
    ...     client_id="your-client-id",
    ...     client_secret="your-client-secret"
    ... )
    >>> fs.ls("")
    """

    protocol = ("onelake", "abfss")

    def __init__(
        self,
        account_name: str = None,
        workspace_name: str = None,
        lakehouse_name: str = None,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
        credential=None,
        anon: bool = False,
        loop=None,
        asynchronous: bool = False,
        **kwargs,
    ):
        # Import here to avoid circular imports
        from fsspec.asyn import get_loop

        super().__init__(asynchronous=asynchronous, loop=loop or get_loop(), **kwargs)

        self.account_name = account_name or "onelake"
        self.workspace_name = workspace_name
        self.lakehouse_name = lakehouse_name
        self.tenant_id = tenant_id or os.getenv("AZURE_TENANT_ID")
        self.client_id = client_id or os.getenv("AZURE_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("AZURE_CLIENT_SECRET")
        self.credential = credential
        self.anon = anon

        # OneLake uses a specific endpoint format
        self.account_url = f"https://{self.account_name}.dfs.fabric.microsoft.com"

        self._setup_credentials()
        self.do_connect()

    def _setup_credentials(self):
        """Setup authentication credentials for OneLake access."""
        if (
            not self.anon
            and not self.credential
            and self.client_id
            and self.client_secret
            and self.tenant_id
        ):
            from azure.identity import ClientSecretCredential
            from azure.identity.aio import (
                ClientSecretCredential as AIOClientSecretCredential,
            )

            self.credential = AIOClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            self.sync_credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )

    def do_connect(self):
        """Establish connection to OneLake service."""
        import weakref

        if self.credential:
            self.service_client = AIODataLakeServiceClient(
                account_url=self.account_url, credential=self.credential
            )
        elif self.anon:
            self.service_client = AIODataLakeServiceClient(account_url=self.account_url)
        else:
            raise ValueError(
                "OneLake requires authentication. Provide credentials or set anon=True"
            )

        # Setup cleanup for service client
        weakref.finalize(self, self._cleanup_service_client, self.service_client)

    @staticmethod
    def _cleanup_service_client(service_client):
        """Cleanup service client resources"""
        try:
            if hasattr(service_client, "close"):
                # For sync cleanup, we need to use asyncio
                import asyncio

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Can't run cleanup in same loop, schedule it
                        loop.create_task(service_client.close())
                    else:
                        loop.run_until_complete(service_client.close())
                except Exception:
                    # If we can't clean up properly, at least try to close the session
                    pass
        except Exception:
            pass

    @classmethod
    def _strip_protocol(cls, path: str):
        """Remove the protocol from the path."""
        # Handle both onelake:// and abfss:// protocols
        if path.startswith("onelake://"):
            path = path[10:]
        elif path.startswith("abfss://"):
            # For abfss URLs, we need to parse the URL and extract the path
            from urllib.parse import urlparse

            parsed = urlparse(path)
            if "onelake.dfs.fabric.microsoft.com" in parsed.netloc:
                # Extract workspace from username part and combine with path
                if "@" in parsed.netloc:
                    workspace = parsed.netloc.split("@")[0]
                    path = f"{workspace}{parsed.path}"
                else:
                    path = parsed.path
            else:
                # Not a OneLake URL, return as-is without protocol
                path = path[8:]  # Remove "abfss://"
        return path.lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        """Extract parameters from OneLake URLs."""
        ops = infer_storage_options(urlpath)
        out = {}

        # Parse OneLake-specific URL structure
        host = ops.get("host", "")
        if "onelake.dfs.fabric.microsoft.com" in host:
            out["account_name"] = "onelake"

            # Check if we have username (from abfss://username@host format)
            username = ops.get("username")
            if username:
                # For abfss URLs, workspace is the username part
                out["workspace_name"] = username

                # Lakehouse is the first part of the path
                path_parts = ops.get("path", "").strip("/").split("/")
                if len(path_parts) >= 1 and path_parts[0]:
                    out["lakehouse_name"] = path_parts[0]
            else:
                # For regular onelake:// URLs, extract from path
                path_parts = ops.get("path", "").strip("/").split("/")
                if len(path_parts) >= 1 and path_parts[0]:
                    out["workspace_name"] = path_parts[0]
                if len(path_parts) >= 2 and path_parts[1]:
                    out["lakehouse_name"] = path_parts[1]

        return out

    def split_path(self, path: str):
        """Split OneLake path into workspace, lakehouse, and file path components."""
        path = self._strip_protocol(path).strip("/")

        if not path:
            return "", "", ""

        parts = path.split("/", 2)
        workspace = parts[0] if len(parts) > 0 else ""
        lakehouse = parts[1] if len(parts) > 1 else ""
        file_path = parts[2] if len(parts) > 2 else ""

        return workspace, lakehouse, file_path

    async def _ls(self, path: str = "", detail: bool = True, **kwargs):
        """List files and directories in OneLake."""
        path = self._strip_protocol(path).strip("/")
        workspace, lakehouse, file_path = self.split_path(path)

        if not workspace:
            # List workspaces (this would require Fabric API, simplified for now)
            if self.workspace_name:
                return [
                    {"name": self.workspace_name, "type": "directory", "size": None}
                ]
            else:
                raise NotImplementedError(
                    "Listing all workspaces requires Fabric API access"
                )

        if not lakehouse:
            # List lakehouses in workspace (simplified)
            if self.lakehouse_name:
                full_path = f"{workspace}/{self.lakehouse_name}"
                return [{"name": full_path, "type": "directory", "size": None}]
            else:
                raise NotImplementedError(
                    "Listing lakehouses requires Fabric API access"
                )

        # For OneLake, the file system is the workspace, and lakehouse is part of the path
        file_system_name = workspace
        lakehouse_path = f"{lakehouse}/{file_path}" if file_path else lakehouse

        try:
            async with self.service_client.get_file_system_client(
                file_system_name
            ) as file_system_client:
                paths = file_system_client.get_paths(
                    path=lakehouse_path, recursive=False
                )

                results = []
                async for path_item in paths:
                    # Construct the full path correctly based on the Azure response
                    full_name = f"{workspace}/{path_item.name}"
                    results.append(
                        {
                            "name": full_name,
                            "type": "directory" if path_item.is_directory else "file",
                            "size": path_item.content_length
                            if hasattr(path_item, "content_length")
                            else None,
                            "last_modified": path_item.last_modified
                            if hasattr(path_item, "last_modified")
                            else None,
                        }
                    )

                return results if detail else [r["name"] for r in results]

        except ResourceNotFoundError:
            raise FileNotFoundError(f"Path not found: {path}")

    async def _info(self, path: str, **kwargs):
        """Get information about a file or directory."""
        path = self._strip_protocol(path).strip("/")
        workspace, lakehouse, file_path = self.split_path(path)

        if not workspace or not lakehouse:
            return {"name": path, "type": "directory", "size": None}

        # For OneLake, the file system is the workspace, and lakehouse is part of the path
        file_system_name = workspace
        lakehouse_file_path = f"{lakehouse}/{file_path}" if file_path else lakehouse

        try:
            async with self.service_client.get_file_system_client(
                file_system_name
            ) as file_system_client:
                if file_path:
                    async with file_system_client.get_file_client(
                        lakehouse_file_path
                    ) as file_client:
                        properties = await file_client.get_file_properties()
                        return {
                            "name": path,
                            "type": "file",
                            "size": properties.size,
                            "last_modified": properties.last_modified,
                        }
                else:
                    # Directory
                    return {"name": path, "type": "directory", "size": None}

        except ResourceNotFoundError:
            raise FileNotFoundError(f"Path not found: {path}")

    async def _cat_file(self, path: str, start: int = None, end: int = None, **kwargs):
        """Read file content from OneLake."""
        path = self._strip_protocol(path).strip("/")
        workspace, lakehouse, file_path = self.split_path(path)

        if not workspace or not lakehouse or not file_path:
            raise ValueError("Invalid path format for OneLake file")

        # For OneLake, the file system is the workspace, and lakehouse is part of the path
        file_system_name = workspace
        lakehouse_file_path = f"{lakehouse}/{file_path}"

        try:
            async with self.service_client.get_file_system_client(
                file_system_name
            ) as file_system_client:
                async with file_system_client.get_file_client(
                    lakehouse_file_path
                ) as file_client:
                    download_stream = await file_client.download_file(
                        offset=start, length=end - start if end else None
                    )
                    return await download_stream.readall()

        except ResourceNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")

    async def _pipe_file(self, path: str, data: bytes, **kwargs):
        """Write data to a file in OneLake."""
        path = self._strip_protocol(path).strip("/")
        workspace, lakehouse, file_path = self.split_path(path)

        if not workspace or not lakehouse or not file_path:
            raise ValueError("Invalid path format for OneLake file")

        # For OneLake, the file system is the workspace, and lakehouse is part of the path
        file_system_name = workspace
        lakehouse_file_path = f"{lakehouse}/{file_path}"

        try:
            async with self.service_client.get_file_system_client(
                file_system_name
            ) as file_system_client:
                async with file_system_client.get_file_client(
                    lakehouse_file_path
                ) as file_client:
                    await file_client.create_file()
                    await file_client.append_data(data, offset=0, length=len(data))
                    await file_client.flush_data(len(data))

        except Exception as e:
            raise IOError(f"Failed to write file {path}: {e}")

    async def _rm_file(self, path: str, **kwargs):
        """Delete a file from OneLake."""
        path = self._strip_protocol(path).strip("/")
        workspace, lakehouse, file_path = self.split_path(path)

        if not workspace or not lakehouse or not file_path:
            raise ValueError("Invalid path format for OneLake file")

        # For OneLake, the file system is the workspace, and lakehouse is part of the path
        file_system_name = workspace
        lakehouse_file_path = f"{lakehouse}/{file_path}"

        try:
            async with self.service_client.get_file_system_client(
                file_system_name
            ) as file_system_client:
                async with file_system_client.get_file_client(
                    lakehouse_file_path
                ) as file_client:
                    await file_client.delete_file()

        except ResourceNotFoundError:
            pass  # File already deleted

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs):
        """Create a directory in OneLake."""
        path = self._strip_protocol(path).strip("/")
        workspace, lakehouse, file_path = self.split_path(path)

        if not workspace or not lakehouse:
            # Can't create workspaces or lakehouses through this API
            raise NotImplementedError(
                "Creating workspaces/lakehouses requires Fabric API"
            )

        # For OneLake, the file system is the workspace, and lakehouse is part of the path
        file_system_name = workspace
        lakehouse_dir_path = f"{lakehouse}/{file_path}"

        try:
            async with self.service_client.get_file_system_client(
                file_system_name
            ) as file_system_client:
                async with file_system_client.get_directory_client(
                    lakehouse_dir_path
                ) as directory_client:
                    await directory_client.create_directory()

        except Exception as e:
            raise IOError(f"Failed to create directory {path}: {e}")

    # Sync wrappers
    ls = sync_wrapper(_ls)
    info = sync_wrapper(_info)
    cat_file = sync_wrapper(_cat_file)
    pipe_file = sync_wrapper(_pipe_file)
    rm_file = sync_wrapper(_rm_file)
    mkdir = sync_wrapper(_mkdir)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: dict = None,
        **kwargs,
    ):
        """Open a file for reading or writing."""
        return OneLakeFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options or {},
            **kwargs,
        )


class OneLakeFile(AbstractBufferedFile):
    """File-like operations on OneLake files."""

    def __init__(
        self,
        fs: OneLakeFileSystem,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: dict = None,
        **kwargs,
    ):
        self.fs = fs
        self.path = path
        self.mode = mode
        self.autocommit = autocommit

        workspace, lakehouse, file_path = fs.split_path(path)
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.file_path = file_path

        if not workspace or not lakehouse or not file_path:
            raise ValueError("Invalid OneLake path format")

        self.container_path = f"{workspace}/{lakehouse}"

        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options or {},
            **kwargs,
        )

    def _fetch_range(self, start: int, end: int):
        """Fetch a range of bytes from the file."""
        return sync(self.fs.loop, self.fs._cat_file, self.path, start=start, end=end)

    def _upload_chunk(self, final: bool = False, **kwargs):
        """Upload a chunk of data."""
        if self.mode in {"wb", "ab"}:
            data = self.buffer.getvalue()
            if data or final:
                sync(self.fs.loop, self.fs._pipe_file, self.path, data)
                self.offset = self.offset + len(data) if self.offset else len(data)
                return True
        return False
