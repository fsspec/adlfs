# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import logging
from os.path import join
from azure.datalake.store import lib, AzureDLFileSystem
from azure.datalake.store.core import AzureDLPath, AzureDLFile
from azure.storage.blob import BlockBlobService, BlobPrefix, Container, BlobBlock
from azure.storage.common._constants import SERVICE_HOST_BASE, DEFAULT_PROTOCOL
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options
from fsspec.utils import tokenize

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
        return self.azure_fs.ls(
            path=path, detail=detail, invalidate_cache=invalidate_cache
        )

    def info(self, path, invalidate_cache=True, expected_error_code=404, **kwargs):
        info = self.azure_fs.info(
            path=path,
            invalidate_cache=invalidate_cache,
            expected_error_code=expected_error_code,
        )
        info["size"] = info["length"]
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

    def seek(self, loc, whence=0, **kwargs):
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
    account_name:
        The storage account name. This is used to authenticate requests
        signed with an account key and to construct the storage endpoint. It
        is required unless a connection string is given, or if a custom
        domain is used with anonymous authentication.
    account_key:
        The storage account key. This is used for shared key authentication.
        If neither account key or sas token is specified, anonymous access
        will be used.
    sas_token:
        A shared access signature token to use to authenticate requests
        instead of the account key. If account key and sas token are both
        specified, account key will be used to sign. If neither are
        specified, anonymous access will be used.
    is_emulated:
        Whether to use the emulator. Defaults to False. If specified, will
        override all other parameters besides connection string and request
        session.
    protocol:
        The protocol to use for requests. Defaults to https.
    endpoint_suffix:
        The host base component of the url, minus the account name. Defaults
        to Azure (core.windows.net). Override this to use the China cloud
        (core.chinacloudapi.cn).
    custom_domain:
        The custom domain to use. This can be set in the Azure Portal. For
        example, 'www.mydomain.com'.
    request_session:
        The session object to use for http requests.
    connection_string:
        If specified, this will override all other parameters besides
        request session. See
        http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
        for the connection string format.
    socket_timeout:
        If specified, this will override the default socket timeout. The timeout specified is in seconds.
        See DEFAULT_SOCKET_TIMEOUT in _constants.py for the default value.
    token_credential:
        A token credential used to authenticate HTTPS requests. The token value
        should be updated before its expiration.
    blocksize:
        The block size to use for download/upload operations. Defaults to the value of
        ``BlockBlobService.MAX_BLOCK_SIZE``

    Examples
    --------
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX")
    >>> adl.ls('')

    **  Sharded Parquet & csv files can be read as: **
        ------------------------------------------
        ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY,})

        ddf = dd.read_parquet('abfs://container_name/folder.parquet', storage_options={
        ...    'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY,})
    """

    protocol = "abfs"

    def __init__(
        self,
        account_name: str,
        account_key: str = None,
        custom_domain: str = None,
        is_emulated: bool = False,
        sas_token: str = None,
        protocol=DEFAULT_PROTOCOL,
        endpoint_suffix=SERVICE_HOST_BASE,
        request_session=None,
        connection_string: str = None,
        socket_timeout=None,
        token_credential=None,
        blocksize=BlockBlobService.MAX_BLOCK_SIZE,
    ):
        AbstractFileSystem.__init__(self)
        self.account_name = account_name
        self.account_key = account_key
        self.custom_domain = custom_domain
        self.is_emulated = is_emulated
        self.sas_token = sas_token
        self.protocol = protocol
        self.endpoint_suffix = endpoint_suffix
        self.request_session = request_session
        self.connection_string = connection_string
        self.socket_timeout = socket_timeout
        self.token_credential = token_credential
        self.blocksize = blocksize
        self.do_connect()

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)

        # we need to make sure that the path retains
        # the format {host}/{path}
        # here host is the container_name
        if ops.get("host", None):
            ops["path"] = ops["host"] + ops["path"]
        ops["path"] = ops["path"].lstrip("/")

        logging.debug(f"_strip_protocol({path}) = {ops}")
        return ops["path"]

    def do_connect(self):
        self.blob_fs = BlockBlobService(
            account_name=self.account_name,
            account_key=self.account_key,
            custom_domain=self.custom_domain,
            is_emulated=self.is_emulated,
            sas_token=self.sas_token,
            protocol=self.protocol,
            endpoint_suffix=self.endpoint_suffix,
            request_session=self.request_session,
            connection_string=self.connection_string,
            socket_timeout=self.socket_timeout,
            token_credential=self.token_credential,
        )

    def split_path(self, path, delimiter="/"):
        """
        Normalize ABFS path string into bucket and key.
        Parameters
        ----------
        path : string
            Input path, like `abfs://my_container/path/to/file`
        Examples
        --------
        >>> split_path("abfs://my_container/path/to/file")
        ['my_container', 'path/to/file']
        """
        path = self._strip_protocol(path)
        path = path.lstrip(delimiter)
        if "/" not in path:
            # this means path is the container_name
            return path, ""
        else:
            return path.split(delimiter, 1)

    def _generate_blobs(self, *args, **kwargs):
        """Follow next_marker to get ALL results."""

        blobs = self.blob_fs.list_blobs(*args, **kwargs)
        yield from blobs
        while blobs.next_marker:
            logging.debug(f"following next_marker {blobs.next_marker}")
            kwargs["marker"] = blobs.next_marker
            blobs = self.blob_fs.list_blobs(*args, **kwargs)
            yield from blobs

    def _matches(self, container_name, path, as_directory=False, delimiter="/"):
        """check if the path returns an exact match"""

        path = path.rstrip(delimiter)
        gen = self.blob_fs.list_blob_names(
            container_name=container_name,
            prefix=path,
            delimiter=delimiter,
            num_results=None,
        )

        contents = list(gen)
        if not contents:
            return False

        if as_directory:
            return contents[0] == path + delimiter
        else:
            return contents[0] == path

    def ls(
        self,
        path: str,
        detail: bool = False,
        invalidate_cache: bool = True,
        delimiter: str = "/",
        **kwargs,
    ):
        """ Create a list of blob names from a blob container

        Parameters
        ----------
        path:  Path to an Azure Blob with its container name
        detail:  If False, return a list of blob names, else a list of dictionaries with blob details
        invalidate_cache:  Boolean
        """
        logging.debug(f"abfs.ls() is searching for {path}")

        path = self._strip_protocol(path).rstrip(delimiter)

        if path in ["", delimiter]:
            logging.debug(f"listing all containers")
            contents = self.blob_fs.list_containers()

            if detail:
                return self._details(contents)
            else:
                return [c.name + delimiter for c in contents]

        else:
            container_name, path = self.split_path(path, delimiter="/")

            # show all top-level prefixes (directories) and files
            if not path:
                if container_name + delimiter not in self.ls(""):
                    raise FileNotFoundError(container_name)

                logging.debug(f"{path} appears to be a container")
                contents = self._generate_blobs(
                    container_name=container_name,
                    prefix=None,
                    delimiter=delimiter,
                    num_results=None,
                )

            # check whether path matches a directory
            # then return the contents
            elif self._matches(
                container_name, path, as_directory=True, delimiter=delimiter
            ):
                logging.debug(f"{path} appears to be a directory")
                dirpath = path + delimiter
                contents = self._generate_blobs(
                    container_name=container_name,
                    prefix=dirpath,
                    delimiter=delimiter,
                    num_results=None,
                )

            # check whether path returns a matching blob
            elif self._matches(
                container_name, path, as_directory=False, delimiter=delimiter
            ):
                # do not use _generate_blobs because we just confirmed
                # there is a single exact match
                logging.debug(f"{path} appears to be a blob (file)")
                contents = self.blob_fs.list_blobs(
                    container_name=container_name,
                    prefix=path,
                    delimiter=delimiter,
                    num_results=1,
                )

            else:
                raise FileNotFoundError(join(container_name, path))

            if detail:
                return self._details(contents, container_name, delimiter=delimiter)
            else:
                return [join(container_name, c.name) for c in contents if c]

    def _details(self, contents, container_name=None, delimiter="/"):
        pathlist = []
        for c in contents:
            data = {}
            if container_name:
                data["name"] = join(container_name, c.name)
            else:
                data["name"] = c.name + delimiter

            if isinstance(c, BlobPrefix):
                data["type"] = "directory"
                data["size"] = 0
            elif isinstance(c, Container):
                data["type"] = "directory"
                data["size"] = 0
            elif c.properties.content_settings.content_type is not None:
                data["type"] = "file"
                data["size"] = c.properties.content_length

            pathlist.append(data)
        return pathlist

    def mkdir(self, path, delimiter="/"):
        container_name, path = self.split_path(path, delimiter=delimiter)
        if (container_name not in self.ls("")) and (not path):
            # create new container
            self.blob_fs.create_container(container_name)
        elif (container_name in self.ls("")) and path:
            ## attempt to create prefix
            raise RuntimeError(
                f"Cannot create {container_name}{delimiter}{path}. Azure does not support creating empty directories."
            )
        else:
            ## everything else
            raise RuntimeError(f"Cannot create {container_name}{delimiter}{path}.")

    def rmdir(self, path, delimiter="/"):
        container_name, path = self.split_path(path, delimiter=delimiter)
        if (container_name + delimiter in self.ls("")) and (not path):
            # delete container
            self.blob_fs.delete_container(container_name)

    def _rm(self, path, delimiter="/"):
        if self.isfile(path):
            container_name, path = self.split_path(path, delimiter=delimiter)
            logging.debug(f"Delete blob {path} in {container_name}")
            self.blob_fs.delete_blob(container_name, path)
        elif self.isdir(path):
            container_name, path = self.split_path(path, delimiter=delimiter)
            if (container_name + delimiter in self.ls("")) and (not path):
                logging.debug(f"Delete container {container_name}")
                self.blob_fs.delete_container(container_name)
        else:
            raise RuntimeError(f"cannot delete {path}")

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """ Open a file on the datalake, or a block blob """
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
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs,
    ):
        container_name, blob = fs.split_path(path)
        self.container_name = container_name
        self.blob = blob

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

    def _fetch_range(self, start, end, **kwargs):
        blob = self.fs.blob_fs.get_blob_to_bytes(
            container_name=self.container_name,
            blob_name=self.blob,
            start_range=start,
            end_range=end,
        )
        return blob.content

    def _initiate_upload(self):
        self._block_list = []
        if self.fs.blob_fs.exists(self.container_name, self.blob):
            self.fs.blob_fs.delete_blob(self.container_name, self.blob)
        return super()._initiate_upload()

    def _upload_chunk(self, final=False, **kwargs):
        data = self.buffer.getvalue()
        block_id = len(self._block_list)
        block_id = f"{block_id:07d}"
        self.fs.blob_fs.put_block(
            container_name=self.container_name,
            blob_name=self.blob,
            block=data,
            block_id=block_id,
        )
        self._block_list.append(block_id)

        if final:
            block_list = [BlobBlock(_id) for _id in self._block_list]
            self.fs.blob_fs.put_block_list(
                container_name=self.container_name,
                blob_name=self.blob,
                block_list=block_list,
            )
