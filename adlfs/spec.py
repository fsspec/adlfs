# -*- coding: utf-8 -*-


from __future__ import absolute_import, division, print_function

import io
from glob import has_magic
import logging
import warnings

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.storage.blob._shared.base_client import create_configuration
from azure.datalake.store import AzureDLFileSystem, lib
from azure.datalake.store.core import AzureDLFile, AzureDLPath
from azure.storage.blob.aio import BlobServiceClient as AIOBlobServiceClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio._models import BlobPrefix
from azure.storage.blob._models import BlobBlock, BlobProperties
from fsspec import AbstractFileSystem
from fsspec.asyn import (
    sync,
    AsyncFileSystem,
    get_loop,
)
from fsspec.utils import infer_storage_options, tokenize, other_paths


logger = logging.getLogger(__name__)


class AzureDatalakeFileSystem(AbstractFileSystem):
    """
    Access Azure Datalake Gen1 as if it were a file system.

    This exposes a filesystem-like API on top of Azure Datalake Storage

    Parameters
    -----------
    tenant_id:  string
        Azure tenant, also known as the subscription id
    client_id: string
        The username or serivceprincipal id
    client_secret: string
        The access key
    store_name: string (optional)
        The name of the datalake account being accessed.  Should be inferred from the urlpath
        if using with Dask read_xxx and to_xxx methods.

    Examples
    --------
    >>> adl = AzureDatalakeFileSystem(tenant_id="xxxx", client_id="xxxx",
    ...                               client_secret="xxxx")

    >>> adl.ls('')

    Sharded Parquet & CSV files can be read as

    >>> storage_options = dict(tennant_id=TENNANT_ID, client_id=CLIENT_ID,
    ...                        client_secret=CLIENT_SECRET)  # doctest: +SKIP
    >>> ddf = dd.read_parquet('adl://store_name/folder/filename.parquet',
    ...                       storage_options=storage_options)  # doctest: +SKIP

    >>> ddf = dd.read_csv('adl://store_name/folder/*.csv'
    ...                   storage_options=storage_options)  # doctest: +SKIP


    Sharded Parquet and CSV files can be written as

    >>> ddf.to_parquet("adl://store_name/folder/filename.parquet",
    ...                storage_options=storage_options)  # doctest: +SKIP

    >>> ddf.to_csv('adl://store_name/folder/*.csv'
    ...            storage_options=storage_options)  # doctest: +SKIP
    """

    protocol = "adl"

    def __init__(self, tenant_id, client_id, client_secret, store_name):
        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = store_name
        self.do_connect()

    @staticmethod
    def _get_kwargs_from_urls(paths):
        """ Get the store_name from the urlpath and pass to storage_options """
        ops = infer_storage_options(paths)
        out = {}
        if ops.get("host", None):
            out["store_name"] = ops["host"]
        return out

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        return ops["path"]

    def do_connect(self):
        """Establish connection object."""
        token = lib.auth(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        self.azure_fs = AzureDLFileSystem(token=token, store_name=self.store_name)

    def ls(self, path, detail=False, invalidate_cache=True, **kwargs):
        files = self.azure_fs.ls(
            path=path, detail=detail, invalidate_cache=invalidate_cache
        )

        for file in files:
            if "type" in file:
                file["type"] = file["type"].lower()
            if "length" in file:
                file["size"] = file["length"]
        return files

    def info(self, path, invalidate_cache=True, expected_error_code=404, **kwargs):
        info = self.azure_fs.info(
            path=path,
            invalidate_cache=invalidate_cache,
            expected_error_code=expected_error_code,
        )
        info["size"] = info["length"]
        """Azure FS uses upper case type values but fsspec is expecting lower case"""
        info["type"] = info["type"].lower()
        return info

    def _trim_filename(self, fn, **kwargs):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        fileparts = so["path"]
        return fileparts

    def glob(self, path, details=False, invalidate_cache=True, **kwargs):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        filepaths = self.azure_fs.glob(
            adlpaths, details=details, invalidate_cache=invalidate_cache
        )
        return filepaths

    def isdir(self, path, **kwargs):
        """Is this entry directory-like?"""
        try:
            return self.info(path)["type"].lower() == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path, **kwargs):
        """Is this entry file-like?"""
        try:
            return self.azure_fs.info(path)["type"].lower() == "file"
        except Exception:
            return False

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options: dict = {},
        **kwargs,
    ):
        return AzureDatalakeFile(self, path, mode=mode)

    def read_block(self, fn, offset, length, delimiter=None, **kwargs):
        return self.azure_fs.read_block(fn, offset, length, delimiter)

    def ukey(self, path):
        return tokenize(self.info(path)["modificationTime"])

    def size(self, path):
        return self.info(path)["length"]

    def __getstate__(self):
        dic = self.__dict__.copy()
        logger.debug("Serialize with state: %s", dic)
        return dic

    def __setstate__(self, state):
        logger.debug("De-serialize with state: %s", state)
        self.__dict__.update(state)
        self.do_connect()


class AzureDatalakeFile(AzureDLFile):
    # TODO: refoctor this. I suspect we actually want to compose an
    # AbstractBufferedFile with an AzureDLFile.

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        autocommit=True,
        block_size=2 ** 25,
        cache_type="bytes",
        cache_options=None,
        *,
        delimiter=None,
        **kwargs,
    ):
        super().__init__(
            azure=fs.azure_fs,
            path=AzureDLPath(path),
            mode=mode,
            blocksize=block_size,
            delimiter=delimiter,
        )
        self.fs = fs
        self.path = AzureDLPath(path)
        self.mode = mode

    def seek(self, loc: int, whence: int = 0, **kwargs):
        """Set current file location

        Parameters
        ----------
        loc: int
            byte location

        whence: {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        loc = int(loc)
        if not self.mode == "rb":
            raise ValueError("Seek only available in read mode")
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError("Seek before start of file")
        self.loc = nloc
        return self.loc


class AzureBlobFileSystem(AsyncFileSystem):
    """
    Access Azure Datalake Gen2 and Azure Storage if it were a file system using Multiprotocol Access

    Parameters
    ----------
    account_name: str
        The storage account name. This is used to authenticate requests
        signed with an account key and to construct the storage endpoint. It
        is required unless a connection string is given, or if a custom
        domain is used with anonymous authentication.
    account_key: str
        The storage account key. This is used for shared key authentication.
        If any of account key, sas token or client_id is specified, anonymous access
        will be used.
    sas_token: str
        A shared access signature token to use to authenticate requests
        instead of the account key. If account key and sas token are both
        specified, account key will be used to sign. If any of account key, sas token
        or client_id are specified, anonymous access will be used.
    request_session: Session
        The session object to use for http requests.
    connection_string: str
        If specified, this will override all other parameters besides
        request session. See
        http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
        for the connection string format.
    socket_timeout: int
        If specified, this will override the default socket timeout. The timeout specified is in
        seconds.
        See DEFAULT_SOCKET_TIMEOUT in _constants.py for the default value.
    credential: TokenCredential or SAS token
        The credentials with which to authenticate.  Optional if the account URL already has a SAS token.
        Can include an instance of TokenCredential class from azure.identity
    blocksize: int
        The block size to use for download/upload operations. Defaults to the value of
        ``BlockBlobService.MAX_BLOCK_SIZE``
    client_id: str
        Client ID to use when authenticating using an AD Service Principal client/secret.
    client_secret: str
        Client secret to use when authenticating using an AD Service Principal client/secret.
    tenant_id: str
        Tenant ID to use when authenticating using an AD Service Principal client/secret.
    default_fill_cache: bool = True
        Whether to use cache filling with opoen by default
    default_cache_type: string ('bytes')
        If given, the default cache_type value used for "open()".  Set to none if no caching
        is desired.  Docs in fsspec

    Pass on to fsspec:

    skip_instance_cache:  to control reuse of instances
    use_listings_cache, listings_expiry_time, max_paths: to control reuse of directory listings

    Examples
    --------
    Authentication with an account_key
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")
    >>> abfs.ls('')

    **  Sharded Parquet & csv files can be read as: **
        ------------------------------------------
        ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY})

        ddf = dd.read_parquet('abfs://container_name/folder.parquet', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY,})

    Authentication with an Azure ServicePrincipal
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", tenant_id=TENANT_ID,
        ...    client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    >>> abfs.ls('')

    **  Read files as: **
        -------------
        ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
            'account_name': ACCOUNT_NAME, 'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET})
        })
    """

    protocol = "abfs"

    def __init__(
        self,
        account_name: str,
        account_key: str = None,
        connection_string: str = None,
        credential: str = None,
        sas_token: str = None,
        request_session=None,
        socket_timeout: int = None,
        blocksize: int = create_configuration(storage_sdk="blob").max_block_size,
        client_id: str = None,
        client_secret: str = None,
        tenant_id: str = None,
        loop=None,
        asynchronous: bool = False,
        default_fill_cache: bool = True,
        default_cache_type: str = "bytes",
        **kwargs,
    ):
        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # pass on to fsspec superclass
        super().__init__(asynchronous=asynchronous, **super_kwargs)
        self.account_name = account_name
        self.account_key = account_key
        self.connection_string = connection_string
        self.credential = credential
        self.sas_token = sas_token
        self.request_session = request_session
        self.socket_timeout = socket_timeout
        self.blocksize = blocksize
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.loop = loop or get_loop()
        self.default_fill_cache = default_fill_cache
        self.default_cache_type = default_cache_type
        if (
            self.credential is None
            and self.account_key is None
            and self.sas_token is None
            and self.client_id is not None
        ):
            self.credential = self._get_credential_from_service_principal()
        self.do_connect()

    @classmethod
    def _strip_protocol(cls, path: str):
        """
        Remove the protocol from the input path

        Parameters
        ----------
        path: str
            Path to remove the protocol from

        Returns
        -------
        str
            Returns a path without the protocol
        """
        logging.debug(f"_strip_protocol for {path}")
        ops = infer_storage_options(path)

        # we need to make sure that the path retains
        # the format {host}/{path}
        # here host is the container_name
        if ops.get("host", None):
            ops["path"] = ops["host"] + ops["path"]
        ops["path"] = ops["path"].lstrip("/")

        logging.debug(f"_strip_protocol({path}) = {ops}")
        return ops["path"]

    def _get_credential_from_service_principal(self):
        """
        Create a Credential for authentication.  This can include a TokenCredential
        client_id, client_secret and tenant_id

        Returns
        -------
        Credential
        """
        from azure.identity import ClientSecretCredential

        sp_token = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return sp_token

    def do_connect(self):
        """Connect to the BlobServiceClient, using user-specified connection details.
        Tries credentials first, then connection string and finally account key

        Raises
        ------
        ValueError if none of the connection details are available
        """
        self.account_url: str = f"https://{self.account_name}.blob.core.windows.net"
        if self.credential is not None:
            self.service_client = AIOBlobServiceClient(
                account_url=self.account_url, credential=self.credential
            )
        elif self.connection_string is not None:
            self.service_client = AIOBlobServiceClient.from_connection_string(
                conn_str=self.connection_string
            )
        elif self.account_key is not None:
            self.service_client = AIOBlobServiceClient(
                account_url=self.account_url, credential=self.account_key
            )
        elif self.sas_token is not None:
            self.service_client = AIOBlobServiceClient(
                account_url=self.account_url + self.sas_token, credential=None
            )
        else:
            raise ValueError("unable to connect with provided params!!")

    def split_path(self, path, delimiter="/", return_container: bool = False, **kwargs):
        """
        Normalize ABFS path string into bucket and key.

        Parameters
        ----------
        path : string
            Input path, like `abfs://my_container/path/to/file`

        delimiter: string
            Delimiter used to split the path

        return_container: bool

        Examples
        --------
        >>> split_path("abfs://my_container/path/to/file")
        ['my_container', 'path/to/file']
        """

        if path in ["", delimiter]:
            return "", ""

        path = self._strip_protocol(path)
        path = path.lstrip(delimiter)
        if "/" not in path:
            # this means path is the container_name
            return path, ""
        else:
            return path.split(delimiter, 1)

    def info(self, path, refresh=False, **kwargs):
        # print(path)
        # print(self._ls_from_cache(""))
        # print(refresh)
        fetch_from_azure = (path and self._ls_from_cache(path) is None) or refresh
        # print(fetch_from_azure)
        if fetch_from_azure:
            return sync(self.loop, self._info, path)
        return super().info(path)

    async def _info(self, path, **kwargs):
        """Give details of entry at path
        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.
        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.
        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.
        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        path = self._strip_protocol(path)
        out = await self._ls(self._parent(path), **kwargs)
        out = [o for o in out if o["name"].rstrip("/") == path]
        if out:
            return out[0]
        out = await self._ls(path, **kwargs)
        path = path.rstrip("/")
        out1 = [o for o in out if o["name"].rstrip("/") == path]
        if len(out1) == 1:
            if "size" not in out1[0]:
                out1[0]["size"] = None
            return out1[0]
        elif len(out1) > 1 or out:
            return {"name": path, "size": 0, "type": "directory"}
        else:
            raise FileNotFoundError

    def glob(self, path, **kwargs):
        return sync(self.loop, self._glob, path)

    async def _glob(self, path, **kwargs):
        """
        Find files by glob-matching.
        If the path ends with '/' and does not contain "*", it is essentially
        the same as ``ls(path)``, returning only files.
        We support ``"**"``,
        ``"?"`` and ``"[..]"``.
        kwargs are passed to ``ls``.
        """
        import re

        ends = path.endswith("/")
        path = self._strip_protocol(path)
        indstar = path.find("*") if path.find("*") >= 0 else len(path)
        indques = path.find("?") if path.find("?") >= 0 else len(path)
        indbrace = path.find("[") if path.find("[") >= 0 else len(path)

        ind = min(indstar, indques, indbrace)

        detail = kwargs.pop("detail", False)

        if not has_magic(path):
            root = path
            depth = 1
            if ends:
                path += "/*"
            elif await self._exists(path):
                if not detail:
                    return [path]
                else:
                    return {path: await self._info(path)}
            else:
                if not detail:
                    return []  # glob of non-existent returns empty
                else:
                    return {}
        elif "/" in path[:ind]:
            ind2 = path[:ind].rindex("/")
            root = path[: ind2 + 1]
            depth = 20 if "**" in path else path[ind2 + 1 :].count("/") + 1
        else:
            root = ""
            depth = 20 if "**" in path else 1

        allpaths = await self._find(
            root, maxdepth=depth, withdirs=True, detail=True, **kwargs
        )
        pattern = (
            "^"
            + (
                path.replace("\\", r"\\")
                .replace(".", r"\.")
                .replace("+", r"\+")
                .replace("//", "/")
                .replace("(", r"\(")
                .replace(")", r"\)")
                .replace("|", r"\|")
                .rstrip("/")
                .replace("?", ".")
            )
            + "$"
        )
        pattern = re.sub("[*]{2}", "=PLACEHOLDER=", pattern)
        pattern = re.sub("[*]", "[^/]*", pattern)
        pattern = re.compile(pattern.replace("=PLACEHOLDER=", ".*"))
        out = {
            p: allpaths[p]
            for p in sorted(allpaths)
            if pattern.match(p.replace("//", "/").rstrip("/"))
        }
        if detail:
            return out
        else:
            return list(out)

    def ls(
        self,
        path: str,
        detail: bool = False,
        invalidate_cache: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        **kwargs,
    ):

        files = sync(
            self.loop,
            self._ls,
            path=path,
            invalidate_cache=invalidate_cache,
            delimiter=delimiter,
            return_glob=return_glob,
        )
        if detail:
            return files
        else:
            return list(sorted(set([f["name"] for f in files])))

    async def _ls(
        self,
        path: str,
        invalidate_cache: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        **kwargs,
    ):
        """
        Create a list of blob names from a blob container

        Parameters
        ----------
        path: str
            Path to an Azure Blob with its container name

        detail: bool
            If False, return a list of blob names, else a list of dictionaries with blob details

        invalidate_cache:  bool
            If True, do not use the cache

        delimiter: str
            Delimiter used to split paths

        return_glob: bool

        """
        logging.debug(f"abfs.ls() is searching for {path}")
        target_path = path.strip("/")
        container, path = self.split_path(path)

        if invalidate_cache:
            self.dircache.clear()

        if (container in ["", ".", delimiter]) and (path in ["", delimiter]):
            if path not in self.dircache or invalidate_cache or return_glob:
                # This is the case where only the containers are being returned
                logging.info(
                    "Returning a list of containers in the azure blob storage account"
                )
                contents = self.service_client.list_containers(include_metadata=True)
                containers = [c async for c in contents]
                files = await self._details(containers)
                self.dircache[path] = files
                return files
            return self.dircache[path]
        else:
            if target_path not in self.dircache or invalidate_cache or return_glob:
                if container not in ["", delimiter]:
                    # This is the case where the container name is passed
                    container_client = self.service_client.get_container_client(
                        container=container
                    )
                    path = path.strip("/")
                    blobs = container_client.walk_blobs(name_starts_with=path)

                    # Check the depth that needs to be screened
                    depth = target_path.count("/")
                    outblobs = []
                    try:
                        async for next_blob in blobs:
                            if depth in [0, 1] and path == "":
                                outblobs.append(next_blob)
                            elif isinstance(next_blob, BlobProperties):
                                if next_blob["name"].count("/") == depth:
                                    outblobs.append(next_blob)
                                elif not next_blob["name"].endswith("/") and (
                                    next_blob["name"].count("/") == (depth - 1)
                                ):
                                    outblobs.append(next_blob)
                            else:
                                async for blob_ in next_blob:
                                    if isinstance(blob_, BlobProperties) or isinstance(
                                        blob_, BlobPrefix
                                    ):
                                        if blob_["name"].endswith("/"):
                                            if (
                                                blob_["name"].rstrip("/").count("/")
                                                == depth
                                            ):
                                                outblobs.append(blob_)
                                        elif blob_["name"].count("/") == (depth):
                                            outblobs.append(blob_)
                                        else:
                                            pass
                    except ResourceNotFoundError:
                        raise FileNotFoundError
                    if return_glob:
                        finalblobs = await self._details(outblobs, return_glob=True)
                        return finalblobs
                    else:
                        finalblobs = await self._details(outblobs)
                    if not finalblobs:
                        if not await self._exists(target_path):
                            raise FileNotFoundError
                        else:
                            return []
                    self.dircache[target_path] = finalblobs
                    return finalblobs
            return self.dircache[target_path]

    async def _details(
        self, contents, delimiter="/", return_glob: bool = False, **kwargs
    ):
        """
        Return a list of dictionaries of specifying details about the contents

        Parameters
        ----------
        contents

        delimiter: str
            Delimiter used to separate containers and files

        return_glob: bool

        Returns
        -------
        List of dicts
            Returns details about the contents, such as name, size and type
        """

        output = []
        for content in contents:
            data = {}
            if content.has_key("container"):  # NOQA
                fname = f"{content.container}{delimiter}{content.name}"
                data.update({"name": fname})
                if content.has_key("size"):  # NOQA
                    data.update({"size": content.size})
                else:
                    data.update({"size": 0})
                if data["size"] == 0 and data["name"].endswith(delimiter):
                    data.update({"type": "directory"})
                else:
                    data.update({"type": "file"})
            else:
                fname = f"{content.name}{delimiter}"
                data.update({"name": fname})
                data.update({"size": 0})
                data.update({"type": "directory"})
            if return_glob:
                data.update({"name": data["name"].rstrip("/")})
            output.append(data)
        return output

    def find(self, path, maxdepth=None, withdirs=False, **kwargs):
        return sync(self.loop, self._find, path, maxdepth, withdirs)

    async def _find(self, path, maxdepth=None, withdirs=False, **kwargs):
        """List all files below path.
        Like posix ``find`` command without conditions
        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.
        """
        # TODO: allow equivalent of -name parameter
        path = self._strip_protocol(path)
        out = dict()
        detail = kwargs.pop("detail", False)
        async for path, dirs, files in self._async_walk(
            path, maxdepth, detail=True, **kwargs
        ):
            if files == []:
                files = {}
                dirs = {}
            if withdirs:
                files.update(dirs)
            out.update({info["name"]: info for name, info in files.items()})
        if await self._isfile(path) and path not in out:
            # walk works on directories, but find should also return [path]
            # when path happens to be a file
            out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        else:
            return {name: out[name] for name in names}

    def _walk(self, path, dirs, files):
        for p, d, f in zip([path], [dirs], [files]):
            yield p, d, f

    async def _async_walk(self, path: str, maxdepth=None, **kwargs):
        """Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into

        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.

        **kwargs are passed to ``ls``
        """
        path = self._strip_protocol(path)
        full_dirs = {}
        dirs = {}
        files = {}

        detail = kwargs.pop("detail", False)
        try:
            listing = await self._ls(path, return_glob=True, **kwargs)
        except (FileNotFoundError, IOError):
            listing = []
            # self._stop_iterating()
            # yield [], [], []

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            pathname = info["name"].rstrip("/")
            name = pathname.rsplit("/", 1)[-1]
            if info["type"] == "directory" and pathname != path:
                # do not include "self" path
                full_dirs[pathname] = info
                dirs[name] = info
            elif pathname == path:
                # file-like with same name as give path
                files[""] = info
            else:
                files[name] = info

        if detail:
            for p, d, f in self._walk(path, dirs, files):
                yield p, d, f
        else:
            yield path, list(dirs), list(files)

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                return

        for d in full_dirs:
            async for path, dirs, files in self._async_walk(
                d, maxdepth=maxdepth, detail=detail, **kwargs
            ):
                yield path, dirs, files

    def mkdir(self, path, delimiter="/", exists_ok=False, **kwargs):
        sync(self.loop, self._mkdir, path, delimiter, exists_ok)

    async def _mkdir(self, path, delimiter="/", exists_ok=False, **kwargs):
        """
        Create directory entry at path

        Parameters
        ----------
        path: str
            The path to create

        delimiter: str
            Delimiter to use when splitting the path

        exists_ok: bool
            If True, raise an exception if the directory already exists. Defaults to False
        """
        container_name, path = self.split_path(path, delimiter=delimiter)
        _containers = await self._ls("")
        _containers = [c["name"] for c in _containers]
        # The list of containers will be returned from _ls() in a directory format,
        # with a trailing "/", but the container_name will not have this.
        # Need a placeholder that presents the container_name in a directory format
        container_name_as_dir = f"{container_name}/"
        if _containers is None:
            _containers = []
        if not exists_ok:
            if (container_name_as_dir not in _containers) and (not path):
                # create new container
                await self.service_client.create_container(name=container_name)
            elif (
                container_name
                in [container_path.split("/")[0] for container_path in _containers]
            ) and path:
                ## attempt to create prefix
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                await container_client.upload_blob(name=path, data="")
            else:
                ## everything else
                raise RuntimeError(f"Cannot create {container_name}{delimiter}{path}.")
        else:
            try:
                if (container_name_as_dir in _containers) and path:
                    container_client = self.service_client.get_container_client(
                        container=container_name
                    )
                    await container_client.upload_blob(name=path, data="")
            except ResourceExistsError:
                pass
        _ = await self._ls("", invalidate_cache=True)

    def rm(self, path, recursive=False, maxdepth=None, **kwargs):
        sync(self.loop, self._rm, path, recursive, **kwargs)

    async def _rm(self, path, recursive=False, maxdepth=None, **kwargs):
        """Delete files.
        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """
        path = await self._expand_path(path, recursive=recursive, maxdepth=maxdepth)
        for p in reversed(path):
            await self.rm_file(p)

    async def rm_file(self, path, delimiter="/", **kwargs):
        """
        Delete a given file

        Parameters
        ----------
        path: str
            Path to file to delete

        delimiter: str
            Delimiter to use when splitting the path
        """
        try:
            kind = await self._info(path)
            kind = kind["type"]
            if kind == "file":
                container_name, path = self.split_path(path, delimiter=delimiter)
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                logging.debug(f"Delete blob {path} in {container_name}")
                await container_client.delete_blob(path)
            elif kind == "directory":
                container_name, path = self.split_path(path, delimiter=delimiter)
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                _containers = await self._ls("")
                _containers = [c["name"] for c in _containers]
                if (container_name + delimiter in _containers) and (not path):
                    logging.debug(f"Delete container {container_name}")
                    await container_client.delete_container()
            else:
                raise RuntimeError(f"Unable to delete {path}!")
            _ = await self._ls("", invalidate_cache=True)

        except FileNotFoundError:
            pass

    def rmdir(self, path: str, delimiter="/", **kwargs):
        sync(self.loop, self._rmdir, path, delimiter=delimiter, **kwargs)

    async def _rmdir(self, path: str, delimiter="/", **kwargs):
        """
        Remove a directory, if empty

        Parameters
        ----------
        path: str
            Path of directory to remove

        delimiter: str
            Delimiter to use when splitting the path

        """

        container_name, path = self.split_path(path, delimiter=delimiter)
        _containers = await self._ls("")
        _containers = [c["name"] for c in _containers]
        if (container_name + delimiter in _containers) and (not path):
            # delete container
            await self.service_client.delete_container(container_name)
            _ = await self._ls("", invalidate_cache=True)

    def size(self, path):
        return sync(self.loop, self._size, path)

    async def _size(self, path):
        """Size in bytes of file"""
        res = await self._info(path)
        size = res.get("size", None)
        return size

    def isfile(self, path):
        return sync(self.loop, self._isfile, path)

    async def _isfile(self, path):
        """Is this entry file-like?"""
        try:
            path_ = path.split("/")[:-1]
            path_ = "/".join([p for p in path_])
            if self.dircache[path_]:
                for fp in self.dircache[path_]:
                    if fp["name"] == path and fp["type"] == "file":
                        return True
        except KeyError:
            pass
        try:
            info = await self._info(path)
            return info["type"] == "file"
        except:  # noqa: E722
            return False

    def isdir(self, path):
        return sync(self.loop, self._isdir, path)

    async def _isdir(self, path):
        """Is this entry directory-like?"""

        if path in self.dircache:
            for fp in self.dircache[path]:
                # Files will contain themselves in the cache, but
                # a directory can not contain itself
                if fp["name"] != path:
                    return True
        try:
            info = await self._info(path)
            return info["type"] == "directory"
        except IOError:
            return False

    def exists(self, path):
        return sync(self.loop, self._exists, path)

    async def _exists(self, path):
        """Is there a file at the given path"""
        try:
            if self._ls_from_cache(path):
                return True
        except FileNotFoundError:
            pass
        try:
            await self._info(path)
            return True
        except:  # noqa: E722
            # any exception allowed bar FileNotFoundError?
            return False

    def expand_path(self, path, recursive=False, maxdepth=None):
        return sync(self.loop, self._expand_path, path, recursive, maxdepth)

    async def _expand_path(self, path, recursive=False, maxdepth=None):
        """Turn one or more globs or directories into a list of all matching files"""
        if isinstance(path, str):
            out = await self._expand_path([path], recursive, maxdepth)
        else:
            out = set()
            for p in path:
                if has_magic(p):
                    bit = set(await self._glob(p))
                    out |= bit
                    if recursive:
                        out += await self._expand_path(p)
                    continue
                elif recursive:
                    out |= set(await self._find(p, withdirs=True))
                # TODO: the following is maybe only necessary if NOT recursive
                out.add(p)
        if not out:
            raise FileNotFoundError
        return list(sorted(out))

    def put(self, lpath, rpath, recursive=False, **kwargs):
        """Copy file(s) from local.
        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.
        Calls put_file for each source.
        """
        from fsspec.implementations.local import make_path_posix, LocalFileSystem

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        fs = LocalFileSystem()
        lpaths = fs.expand_path(lpath, recursive=recursive)
        rpaths = other_paths(lpaths, rpath)

        for lpath, rpath in zip(lpaths, rpaths):
            self.put_file(lpath, rpath, **kwargs)

    def upload(self, lpath, rpath, recursive=False, **kwargs):
        """Alias of :ref:`FilesystemSpec.put`."""
        return self.put(lpath, rpath, recursive=recursive, **kwargs)

    def download(self, rpath, lpath, recursive=False, **kwargs):
        """Alias of :ref:`FilesystemSpec.get`."""
        return self.get(rpath, lpath, recursive=recursive, **kwargs)

    def get(self, rpath, lpath, recursive=False, **kwargs):
        """Copy file(s) to local.
        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.
        Calls get_file for each source.
        """
        from fsspec.implementations.local import make_path_posix

        rpath = self._strip_protocol(rpath)
        if isinstance(lpath, str):
            lpath = make_path_posix(lpath)
        rpaths = self.expand_path(rpath, recursive=recursive)
        lpaths = other_paths(rpaths, lpath)
        for lpath, rpath in zip(lpaths, rpaths):
            self.get_file(rpath, lpath, **kwargs)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: dict = {},
        cache_type="readahead",
        **kwargs,
    ):
        """Open a file on the datalake, or a block blob

        Parameters
        ----------
        path: str
            Path to file to open

        mode: str
            What mode to open the file in - defaults to "rb"

        block_size: int
            Size per block for multi-part downloads.

        autocommit: bool
            Whether or not to write to the destination directly

        cache_type: str
            One of "readahead", "none", "mmap", "bytes", defaults to "readahead"
            Caching policy in read mode.
            See the definitions here:
            https://filesystem-spec.readthedocs.io/en/latest/api.html#readbuffering
        """
        logging.debug(f"_open:  {path}")
        return AzureBlobFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            cache_type=cache_type,
            **kwargs,
        )


class AzureBlobFile(io.IOBase):
    """ File-like operations on Azure Blobs """

    DEFAULT_BLOCK_SIZE = 5 * 2 ** 20

    def __init__(
        self,
        fs: AzureBlobFileSystem,
        path: str,
        mode: str = "rb",
        block_size="default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict = {},
        **kwargs,
    ):
        """
        Represents a file on AzureBlobStorage that implements buffered reading and writing

        Parameters
        ----------
        fs: AzureBlobFileSystem
            An instance of the filesystem

        path: str
            The location of the file on the filesystem

        mode: str
            What mode to open the file in. Defaults to "rb"

        block_size: int, str
            Buffer size for reading and writing. The string "default" will use the class
            default

        autocommit: bool
            Whether or not to write to the destination directly

        cache_type: str
            One of "readahead", "none", "mmap", "bytes", defaults to "readahead"
            Caching policy in read mode. See the definitions in ``core``.

        cache_options : dict
            Additional options passed to the constructor for the cache specified
            by `cache_type`.

        kwargs: dict
            Passed to AbstractBufferedFile
        """

        from fsspec.core import caches

        container_name, blob = fs.split_path(path)
        self.fs = fs
        self.path = path
        self.mode = mode
        self.container_name = container_name
        self.blob = blob
        self.block_size = block_size
        # self.container_client = fs.service_client.get_container_client(
        #     self.container_name
        # )
        self.blocksize = (
            self.DEFAULT_BLOCK_SIZE if block_size in ["default", None] else block_size
        )
        self.loc = 0
        self.autocommit = autocommit
        self.end = None
        self.start = None
        self.closed = False

        if cache_options is None:
            cache_options = {}

        if "trim" in kwargs:
            warnings.warn(
                "Passing 'trim' to control the cache behavior has been deprecated. "
                "Specify it within the 'cache_options' argument instead.",
                FutureWarning,
            )
            cache_options["trim"] = kwargs.pop("trim")

        self.kwargs = kwargs
        self.connect_client()
        if self.mode not in {"ab", "rb", "wb"}:
            raise NotImplementedError("File mode not supported")
        if self.mode == "rb":
            if not hasattr(self, "details"):
                self.details = self.fs.info(self.path)
            self.size = self.details["size"]
            self.cache = caches[cache_type](
                self.blocksize, self._fetch_range, self.size, **cache_options
            )
        else:
            self.buffer = io.BytesIO()
            self.offset = None
            self.forced = False
            self.location = None

    def connect_client(self):
        """Connect to the Synchronous BlobServiceClient, using user-specified connection details.
        Tries credentials first, then connection string and finally account key

        Raises
        ------
        ValueError if none of the connection details are available
        """
        self.fs.account_url: str = (
            f"https://{self.fs.account_name}.blob.core.windows.net"
        )
        if self.fs.credential is not None:
            self.container_client = BlobServiceClient(
                account_url=self.fs.account_url, credential=self.fs.credential
            ).get_container_client(self.container_name)
        elif self.fs.connection_string is not None:
            self.container_client = BlobServiceClient.from_connection_string(
                conn_str=self.fs.connection_string
            ).get_container_client(self.container_name)
        elif self.fs.account_key is not None:
            self.container_client = BlobServiceClient(
                account_url=self.fs.account_url, credential=self.fs.account_key
            ).get_container_client(self.container_name)
        elif self.fs.sas_token is not None:
            self.container_client = BlobServiceClient(
                account_url=self.fs.account_url + self.fs.sas_token, credential=None
            ).get_container_client(self.container_name)
        else:
            raise ValueError("unable to connect with provided params!!")

    @property
    def closed(self):
        # get around this attr being read-only in IOBase
        # use getattr here, since this can be called during del
        return getattr(self, "_closed", True)

    @closed.setter
    def closed(self, c):
        self._closed = c

    def __hash__(self):
        if "w" in self.mode:
            return id(self)
        else:
            return int(tokenize(self.details), 16)

    def __eq__(self, other):
        """Files are equal if they have the same checksum, only in read mode"""
        return self.mode == "rb" and other.mode == "rb" and hash(self) == hash(other)

    def commit(self):
        """Move from temp to final destination"""

    def tell(self):
        """ Current file location """
        return self.loc

    def seek(self, loc, whence=0):
        """Set current file location
        Parameters
        ----------
        loc: int
            byte location
        whence: {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        loc = int(loc)
        if not self.mode == "rb":
            raise OSError("Seek only available in read mode")
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError("Seek before start of file")
        self.loc = nloc
        return self.loc

    def discard(self):
        """Throw away temporary file"""

    def _fetch_range(self, start: int, end: int, **kwargs):
        """
        Download a chunk of data specified by start and end

        Parameters
        ----------
        start: int
            Start byte position to download blob from
        end: int
            End byte position to download blob from
        """
        blob = self.container_client.download_blob(
            blob=self.blob, offset=start, length=end
        )
        return blob.readall()

    def __initiate_upload(self, **kwargs):
        pass

    def _initiate_upload(self, **kwargs):
        """Prepare a remote file upload"""
        self.blob_client = self.container_client.get_blob_client(blob=self.blob)
        self._block_list = []
        try:
            self.container_client.delete_blob(self.blob)
        except ResourceNotFoundError:
            pass
        else:
            return self.__initiate_upload()

    def _upload_chunk(self, final: bool = False, **kwargs):
        """
        Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.

        """
        data = self.buffer.getvalue()
        length = len(data)
        block_id = len(self._block_list)
        block_id = f"{block_id:07d}"
        self.blob_client.stage_block(block_id=block_id, data=data, length=length)
        self._block_list.append(block_id)

        if final:
            block_list = [BlobBlock(_id) for _id in self._block_list]
            self.blob_client.commit_block_list(block_list=block_list)

    def flush(self, force=False):
        """
        Write buffered data to backend store.
        Writes the current buffer, if it is larger than the block-size, or if
        the file is being closed.
        Parameters
        ----------
        force: bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be. Disallows further writing to this file.
        """

        if self.closed:
            raise ValueError("Flush on closed file")
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")
        if force:
            self.forced = True
        if self.mode not in {"wb", "ab"}:
            # no-op to flush on read-mode
            return

        if not force and self.buffer.tell() < self.blocksize:
            # Defer write on small block
            return

        if self.offset is None:
            # Initialize a multipart upload
            self.offset = 0
            self._initiate_upload()

        if self._upload_chunk(final=force) is not False:
            self.offset += self.buffer.seek(0, 2)
            self.buffer = io.BytesIO()

    def write(self, data):
        """
        Write data to buffer.
        Buffer only sent on flush() or if buffer is greater than
        or equal to blocksize.
        Parameters
        ----------
        data: bytes
            Set of bytes to be written.
        """

        if self.mode not in {"wb", "ab"}:
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.forced:
            raise ValueError("This file has been force-flushed, can only close")
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() >= self.blocksize:
            self.flush()
        return out

    def readuntil(self, char=b"\n", blocks=None):
        """Return data between current position and first occurrence of char
        char is included in the output, except if the end of the tile is
        encountered first.
        Parameters
        ----------
        char: bytes
            Thing to find
        blocks: None or int
            How much to read in each go. Defaults to file blocksize - which may
            mean a new read on every call.
        """
        out = []
        while True:
            start = self.tell()
            part = self.read(blocks or self.blocksize)
            if len(part) == 0:
                break
            found = part.find(char)
            if found > -1:
                out.append(part[: found + len(char)])
                self.seek(start + found + len(char))
                break
            out.append(part)
        return b"".join(out)

    def readline(self):
        """Read until first occurrence of newline character
        Note that, because of character encoding, this is not necessarily a
        true line ending.
        """
        return self.readuntil(b"\n")

    def __next__(self):
        out = self.readline()
        if out:
            return out
        raise StopIteration

    def __iter__(self):
        return self

    def readlines(self):
        """Return all data, split by the newline character"""
        data = self.read()
        lines = data.split(b"\n")
        out = [l + b"\n" for l in lines[:-1]]
        if data.endswith(b"\n"):
            return out
        else:
            return out + [lines[-1]]
        # return list(self)  ???

    def readinto(self, b):
        return self.readinto(b)

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        Parameters
        ----------
        length: int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """

        length = -1 if length is None else int(length)
        if self.mode != "rb":
            raise ValueError("File not in read mode")
        if length < 0:
            length = self.size - self.loc
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        logger.debug("%s read: %i - %i" % (self, self.loc, self.loc + length))
        if length == 0:
            # don't even bother calling fetch
            return b""
        out = self.cache._fetch(self.loc, self.loc + length)
        self.loc += len(out)
        return out

    def close(self):
        """Close file
        Finalizes writes, discards cache
        """
        if self.closed:
            return
        if self.mode == "rb":
            self.cache = None
        else:
            if not self.forced:
                self.flush(force=True)
            if self.fs is not None:
                self.fs.invalidate_cache(self.path)
                self.fs.invalidate_cache(self.fs._parent(self.path))

        self.closed = True

    def readable(self):
        """Whether opened for reading"""
        return self.mode == "rb" and not self.closed

    def seekable(self):
        """Whether is seekable (only in read mode)"""
        return self.readable()

    def writable(self):
        """Whether opened for writing"""
        return self.mode in {"wb", "ab"} and not self.closed

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()
