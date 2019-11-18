# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging


from azure.datalake.store import lib, AzureDLFileSystem
from azure.datalake.store.core import AzureDLPath, AzureDLFile
from azure.storage.blob import BlockBlobService
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, stringify_path, tokenize

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
        # Need to determine what information can be deleted
        # before passing to the Dask workers
        # del dic['token']
        # del dic['azure']
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
    storage_account:  Name of the Azure Storage Account
    account_key:  Access key for the Azure Storage account
    container_name:  Name of the container or filesystem to be accessed (optional)

    Examples
    --------
    >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX", container_name="XXXX")
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
        container_name: str,
        account_key: str,
        custom_domain: str = None,
        is_emulated: bool = False,
    ):
        AbstractFileSystem.__init__(self)
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.custom_domain = custom_domain
        self.is_emulated = is_emulated
        self.do_connect()

    @staticmethod
    def _get_kwargs_from_urls(paths):
        """ Get the store_name from the urlpath and pass to storage_options """
        logging.debug("Getting kwargs from urls...")
        ops = infer_storage_options(paths)
        out = {}
        if ops.get("host", None):
            out["container_name"] = ops["host"]
        logging.debug(f"kwargs are:  {out}")
        return out

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        ops["path"] = ops["path"].lstrip("/")
        logging.debug(f"_strip_protocol:  {ops}")
        return ops["path"]

    def do_connect(self):
        self.blob_fs = BlockBlobService(
            account_name=self.account_name,
            account_key=self.account_key,
            custom_domain=self.custom_domain,
            is_emulated=self.is_emulated,
        )

    def ls(
        self, path: str, detail: bool = False, invalidate_cache: bool = True, **kwargs
    ):
        """ Create a list of blob names from a blob container

        Parameters
        ----------
        path:  Path to an Azure Blob directory
        detail:  If False, return a list of blob names, else a list of dictionaries with blob details
        invalidate_cache:  Boolean
        """
        logging.debug("Running abfs.ls() method")
        path = stringify_path(path)
        blobs = self.blob_fs.list_blobs(container_name=self.container_name, prefix=path)
        if detail is False:
            pathlist = [blob.name for blob in blobs]
            logging.debug(f"Detail is False.  Returning {pathlist}")
            return pathlist
        else:
            pathlist = []
            for blob in blobs:
                data = {}
                data["name"] = blob.name
                data["size"] = blob.properties.content_length
                data["container_name"] = self.container_name
                if blob.properties.content_settings.content_type is not None:
                    data["type"] = "file"
                else:
                    data["type"] = "directory"
                pathlist.append(data)
            logging.debug(f"Detail is True:  Returning {pathlist}")
            return pathlist

    def info(self, path: str, **kwargs):
        """ Create a dictionary of path attributes

        Parameters
        ----------
        path:  An Azure Blob
        """
        blob = self.blob_fs.get_blob_properties(
            container_name=self.container_name, blob_name=path
        )
        info = {}
        info["name"] = path
        info["size"] = blob.properties.content_length
        info["container_name"] = self.container_name
        if blob.properties.content_settings.content_type is not None:
            info["type"] = "file"
        else:
            info["type"] = "directory"
        return info

    def walk(self, path, maxdepth=None, **kwargs):
        """ Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.
        Note that the "files" outputted will include anything that is not
        a directory, such as links.  Used by pyarrow during making of the manifest
        of a ParquetDataset

        Parameters
        ----------
        path: str
            Root to recurse into
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.
        kwargs: passed to ``ls``
        """

        logging.debug(f"abfs.walk() for {path}")
        path = self._strip_protocol(path)
        full_dirs = []
        dirs = []
        files = []

        try:
            listing = self.ls(path, True, **kwargs)
        except (FileNotFoundError, IOError):
            return [], [], []

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            name = info["name"].rstrip("/")
            logging.debug(
                f"Test path with name, path, type, size:  {name}, {path}, {info['type']}, {info['size']}"
            )
            if info["type"] == "directory" and name != path and info["size"] == 0:
                logging.debug(f"{name} is a directory")
                logging.debug(f"compare name and path: {name}, {path}")
                # do not include "self" path
                full_dirs.append(name)
                # Need to add this line to handle an oddity in how
                # Azure Storage returns blob paths from list operations.
                # Without it, the ParquetDataset operation by pyarrow fails
                logging.debug(
                    f"Path name:  {name} is a directory.  Evaluate against {path}"
                )
                dirs.append(name.rsplit("/", 1)[-1])
            elif info["type"] == "directory" and name == path and info["size"] == 0:
                logging.debug(f"Skipping {name}.  It is the current directory")
                # The Azure Blob Storage SDK returns the path from a list_blobs()
                # method call.  This creates an inconsistency across dasks's read methods
                # Specifically (read_csv(fpath/fname.csv/*.csv), and the fastparquet vs pyarrow
                # read_parquet(fpath/fname.parquet) implement a glob call.  Adding
                # this line reconciles those differences
                continue
            elif name == path and info["type"] == "file":
                logging.debug(f"name == path")
                # file-like with same name as give path
                files.append("")
            else:
                logging.debug(f"{name} is a file")
                files.append(name.rsplit("/", 1)[-1])
        yield path, dirs, files

        for d in full_dirs:
            if maxdepth is None or maxdepth > 1:
                for res in self.walk(
                    d,
                    maxdepth=(maxdepth - 1) if maxdepth is not None else None,
                    **kwargs,
                ):
                    yield res

    def isdir(self, path, **kwargs):
        """Is this entry directory-like?"""
        try:
            return self.info(path)["type"].lower() == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path, **kwargs):
        """Is this entry file-like?"""
        try:
            return self.info(path)["type"].lower() == "file"
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
        """ Open a file on the datalake, or a block blob """
        logging.debug(f"_open:  {path}")
        return AzureBlobFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
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
        self.container_name = fs.container_name

    def _fetch_range(self, start, end, **kwargs):
        blob = self.fs.blob_fs.get_blob_to_bytes(
            container_name=self.container_name,
            blob_name=self.path,
            start_range=start,
            end_range=end,
        )
        return blob.content

    def _upload_chunk(self, final=False, **kwargs):
        data = self.buffer.getvalue()
        self.fs.blob_fs.create_blob_from_bytes(
            container_name=self.container_name, blob_name=self.path, blob=data
        )
