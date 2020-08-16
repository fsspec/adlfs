# -*- coding: utf-8 -*-


from __future__ import absolute_import, division, print_function

import logging

from azure.core.exceptions import ResourceNotFoundError
from azure.core.paging import ItemPaged
from azure.storage.blob._shared.base_client import create_configuration
from azure.datalake.store import AzureDLFileSystem, lib
from azure.datalake.store.core import AzureDLFile, AzureDLPath
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._models import BlobBlock, BlobPrefix
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, tokenize


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
        cache_options=None,
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
        """ Set current file location

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


class AzureBlobFileSystem(AbstractFileSystem):
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
    ):
        AbstractFileSystem.__init__(self)
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
            self.service_client = BlobServiceClient(
                account_url=self.account_url, credential=self.credential
            )
        elif self.connection_string is not None:
            self.service_client = BlobServiceClient.from_connection_string(
                conn_str=self.connection_string
            )
        elif self.account_key is not None:
            self.service_client = BlobServiceClient(
                account_url=self.account_url, credential=self.account_key
            )
        elif self.sas_token is not None:
            self.service_client = BlobServiceClient(
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

    def ls(
        self,
        path: str,
        detail: bool = False,
        invalidate_cache: bool = True,
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
        # import pdb;pdb.set_trace()
        logging.debug(f"abfs.ls() is searching for {path}")

        target_path = path.lstrip("/")
        container, path = self.split_path(path)
        if (container in ["", ".", delimiter]) and (path in ["", delimiter]):
            # This is the case where only the containers are being returned
            logging.info(
                "Returning a list of containers in the azure blob storage account"
            )
            if detail:
                contents = self.service_client.list_containers(include_metadata=True)
                return self._details(contents)
            else:
                contents = self.service_client.list_containers()
                return [f"{c.name}{delimiter}" for c in contents]

        else:
            if container not in ["", delimiter]:
                # This is the case where the container name is passed
                container_client = self.service_client.get_container_client(
                    container=container
                )
                blobs = container_client.walk_blobs(name_starts_with=path)
                try:
                    blobs = [blob for blob in blobs]
                except Exception:
                    raise FileNotFoundError
                if len(blobs) > 1:
                    if return_glob:
                        return self._details(blobs, return_glob=True)
                    if detail:
                        return self._details(blobs)
                    else:
                        return [
                            f"{blob.container}{delimiter}{blob.name}" for blob in blobs
                        ]
                elif len(blobs) == 1:
                    if (blobs[0].name.rstrip(delimiter) == path) and not blobs[
                        0
                    ].has_key(  # NOQA
                        "blob_type"
                    ):

                        path = blobs[0].name
                        blobs = container_client.walk_blobs(name_starts_with=path)
                        if return_glob:
                            return self._details(blobs, return_glob=True)
                        if detail:
                            return self._details(blobs)
                        else:
                            return [
                                f"{blob.container}{delimiter}{blob.name}"
                                for blob in blobs
                            ]
                    elif isinstance(blobs[0], BlobPrefix):
                        if detail:
                            for blob_page in blobs:
                                return self._details(blob_page)
                        else:
                            outblobs = []
                            depth = target_path.count("/")
                            if depth == 0:
                                depth = 2
                            else:
                                depth = depth + 1
                            for blob_page in blobs:
                                for blob in blob_page:
                                    directory_ = (
                                        f"{blob.container}{delimiter}{blob.name}"
                                    )
                                    dir_parts = directory_.split("/", depth)[0:depth]
                                    directory = "/".join([d for d in dir_parts])
                                    directory = f"{directory}/"
                                    outblobs.append(directory)
                            return outblobs
                    elif blobs[0]["blob_type"] == "BlockBlob":
                        if detail:
                            return self._details(blobs)
                        else:
                            return [
                                f"{blob.container}{delimiter}{blob.name}"
                                for blob in blobs
                            ]
                    elif isinstance(blobs[0], ItemPaged):
                        outblobs = []
                        for page in blobs:
                            for b in page:
                                outblobs.append(b)
                    else:
                        raise FileNotFoundError(
                            f"Unable to identify blobs in {path} for {blobs[0].name}"
                        )
                elif len(blobs) == 0:
                    if return_glob or (path in ["", delimiter]):
                        return []
                    else:
                        raise FileNotFoundError
                else:
                    raise FileNotFoundError

    def _details(self, contents, delimiter="/", return_glob: bool = False, **kwargs):
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
        pathlist = []
        for c in contents:
            data = {}
            if c.has_key("container"):  # NOQA
                data["name"] = f"{c.container}{delimiter}{c.name}"
                if c.has_key("size"):  # NOQA
                    data["size"] = c.size
                else:
                    data["size"] = 0
                if data["size"] == 0:
                    data["type"] = "directory"
                else:
                    data["type"] = "file"
            else:
                data["name"] = f"{c.name}{delimiter}"
                data["size"] = 0
                data["type"] = "directory"
            if return_glob:
                data["name"] = data["name"].rstrip("/")

            pathlist.append(data)
        return pathlist

    def walk(self, path: str, maxdepth=None, **kwargs):
        """ Return all files belows path

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
            listing = self.ls(path, detail=True, return_glob=True, **kwargs)
        except (FileNotFoundError, IOError):
            return [], [], []

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
            yield path, dirs, files
        else:
            yield path, list(dirs), list(files)

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                return

        for d in full_dirs:
            yield from self.walk(d, maxdepth=maxdepth, detail=detail, **kwargs)

    def mkdir(self, path, delimiter="/", exists_ok=False, **kwargs):
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
        if not exists_ok:
            if (container_name not in self.ls("")) and (not path):
                # create new container
                self.service_client.create_container(name=container_name)
            elif (
                container_name
                in [container_path.split("/")[0] for container_path in self.ls("")]
            ) and path:
                ## attempt to create prefix
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                container_client.upload_blob(name=path, data="")
            else:
                ## everything else
                raise RuntimeError(f"Cannot create {container_name}{delimiter}{path}.")
        else:
            if container_name in self.ls("") and path:
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                container_client.upload_blob(name=path, data="")

    def rmdir(self, path: str, delimiter="/", **kwargs):
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
        if (container_name + delimiter in self.ls("")) and (not path):
            # delete container
            self.service_client.delete_container(container_name)

    def rm_file(self, path, delimiter="/", **kwargs):
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
            kind = self.info(path)["type"]
            if kind == "file":
                container_name, path = self.split_path(path, delimiter=delimiter)
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                logging.debug(f"Delete blob {path} in {container_name}")
                container_client.delete_blob(path)
            elif kind == "directory":
                container_name, path = self.split_path(path, delimiter=delimiter)
                container_client = self.service_client.get_container_client(
                    container=container_name
                )
                if (container_name + delimiter in self.ls("")) and (not path):
                    logging.debug(f"Delete container {container_name}")
                    container_client.delete_container()
            else:
                raise RuntimeError(f"Unable to delete {path}!")
        except FileNotFoundError:
            pass

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options=None,
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
            block_size=block_size or self.blocksize,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )


class AzureBlobFile(AbstractBufferedFile):
    """ File-like operations on Azure Blobs """

    def __init__(
        self,
        fs: AzureBlobFileSystem,
        path: str,
        mode: str = "rb",
        block_size="default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict = None,
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
        container_name, blob = fs.split_path(path)
        self.container_name = container_name
        self.blob = blob
        self.container_client = fs.service_client.get_container_client(
            self.container_name
        )

        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )

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

    def _initiate_upload(self, **kwargs):
        """Prepare a remote file upload"""
        self.blob_client = self.container_client.get_blob_client(blob=self.blob)
        self._block_list = []
        try:
            self.container_client.delete_blob(self.blob)
        except ResourceNotFoundError:
            pass
        except Exception as e:
            raise RuntimeError(f"Failed for {e}")
        else:
            return super()._initiate_upload()

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
