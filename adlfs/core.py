# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging
import requests
import re
import os


from azure.datalake.store import lib, AzureDLFileSystem
from azure.datalake.store.core import AzureDLPath, AzureDLFile
from azure.storage.blob import BlockBlobService
from azure.datalake.store.core import _put_data_with_retry
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, stringify_path, tokenize

logger = logging.getLogger(__name__)


class AzureDatalakeFileSystem(AbstractFileSystem):
    
    
    """
    Access Azure Datalake Gen1 as if it were a file system.

    This exposes a filesystem-like API on top of Azure Datalake Storage

    Examples
    _________
    >>> adl = AzureDatalakeFileSystem(tenant_id="xxxx", client_id="xxxx", 
                                    client_secret="xxxx"
                                    )
        adl.ls('')
        
        Sharded Parquet & csv files can be read as:
        ----------------------------
        ddf = dd.read_parquet('adl://store_name/folder/filename.parquet', storage_options={
            'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        })

        ddf = dd.read_csv('adl://store_name/folder/*.csv', storage_options={
            'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        })

        Sharded Parquet and csv files can be written as:
        ------------------------------------------------
        dd.to_parquet(ddf, 'adl://store_name/folder/filename.parquet, storage_options={
            'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        })
        
        ddf.to_csv('adl://store_name/folder/*.csv', storage_options={
            'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        })

    Parameters
    __________
    tenant_id:  string
        Azure tenant, also known as the subscription id
    client_id: string
        The username or serivceprincipal id
    client_secret: string
        The access key
    store_name: string (None)
        The name of the datalake account being accessed.  Should be inferred from the urlpath
        if using with Dask read_xxx and to_xxx methods.
    """

    def __init__(self, tenant_id, client_id, client_secret, store_name):
        AbstractFileSystem.__init__(self)
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
        if ops.get('host', None):
            out['store_name'] = ops['host']
        return out

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        return ops['path']
    
    def do_connect(self):
        """Establish connection object."""
        token = lib.auth(tenant_id=self.tenant_id,
                        client_id=self.client_id,
                        client_secret=self.client_secret,
                        )
        self.azure_fs = AzureDLFileSystem(token=token,
                                   store_name=self.store_name)
        
    def ls(self, path, detail=False, invalidate_cache=True):
        return self.azure_fs.ls(path=path, detail=detail,
                          invalidate_cache=invalidate_cache)
    
    def info(self, path, invalidate_cache=True, expected_error_code=404):
        info = self.azure_fs.info(path=path, invalidate_cache=invalidate_cache, expected_error_code=expected_error_code)
        info['size'] = info['length']
        return info

    def _trim_filename(self, fn):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        fileparts = so['path']
        return fileparts

    def glob(self, path, details=False, invalidate_cache=True):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        filepaths = self.azure_fs.glob(adlpaths, details=details, invalidate_cache=invalidate_cache)
        return filepaths
    
    def isdir(self, path):
        """Is this entry directory-like?"""
        try:
            return self.info(path)['type'].lower() == 'directory'
        except FileNotFoundError:
            return False

    def isfile(self, path):
        """Is this entry file-like?"""
        try:
            return self.azure_fs.info(path)['type'].lower() == 'file'
        except:
            return False

    def _open(self, path, mode='rb', block_size=None, autocommit=True):
        return AzureDatalakeFile(self, path, mode=mode)
    
    def read_block(self, fn, offset, length, delimiter=None):
        return self.azure_fs.read_block(fn, offset, length, delimiter)
        
    def ukey(self, path):
        return tokenize(self.info(path)['modificationTime'])

    def size(self, path):
        return self.info(path)['length']

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
    def __init__(self, fs, path, mode='rb', blocksize=2**25, delimiter=None):
        super().__init__(azure=fs.azure_fs, path=AzureDLPath(path), mode=mode, 
                         blocksize=blocksize, delimiter=delimiter)
        self.fs = fs
        self.path = AzureDLPath(path)
        self.mode = mode
    
    def seek(self, loc, whence=0):
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
    abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<file_name>
    """

    protocol = 'abfs'

    def __init__(self, account_name: str, account_key: str):
        
        """ Access Azure Datalake Gen2 and Azure Storage using Multiprotocol Access
        
        Parameters
        ----------
        storage_account:  Name of the Azure Storage Account
        account_key:  Access keys for the Azure Storage account
        """
        AbstractFileSystem.__init__(self)
        self.account_name = account_name
        self.account_key = account_key
        self.do_connect()
        
    def do_connect(self):
        self.fs = BlockBlobService(account_name=self.account_name, account_key=self.account_key)
        
    def ls(self, path=None, detail=False, invalidate_cache=True):
        path = stringify_path(path)
        parsed_path = path.split('/')
        container_name = parsed_path[0]
        prefix = "/".join(p for p in parsed_path[1:])
        files = self.fs.list_blobs(container_name=container_name, prefix=prefix)
        if detail is False:
            pathlist = [f.name for f in files]
            return pathlist
        # for file in files:
        #     print(file.name, file.metadata, file.properties.blob_type, file.properties.content_length, file.properties.content_settings.content_type)
        else:
            pathlist = []
            for file in files:
                data = {}
                data['name'] = file.name
                data['size'] = file.properties.content_length
                if file.properties.content_settings.content_type is not None:
                    data['type'] = 'file'
                else: 
                    data['type'] = 'directory'
                pathlist.append(data)
            return pathlist
            