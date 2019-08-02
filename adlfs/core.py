# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging
from fsspec import AbstractFileSystem
# from fsspec import AbstractBufferedFile
from azure.datalake.store import lib, AzureDLFileSystem

from fsspec.utils import infer_storage_options

logger = logging.getLogger(__name__)


class AzureDLFileSystem(AbstractFileSystem):
    """
    Access Azure Datalake Gen1 as if it were a file system.

    This exposes a filesystem-like API on top of Azure Datalake Storage

    Examples
    _________
    >>> adl = AzureDatalakeFileSystem(tenant_id="xxxx", client_id="xxxx", 
                                    client_secret="xxxx", storage_name="storage_account"
                                    )
        adl.ls('')

    Parameters
    __________P
    tenant_id:  string
        Azure tenant, also known as the subscription id
    client_id: string
        The username or serivceprincipal id
    client_secret: string
        The access key
    store_name: string (None)
        The name of the datalake account being accessed
    """

    # def __init__(self, tenant_id=None, client_id=None, client_secret=None, store_name=None,
    #             **kwargs):
    #     super().__init__()
    #     self.tenant_id = tenant_id
    #     self.client_id = client_id
    #     self.client_secret = client_secret
    #     self.store_name = store_name
    #     self.kwargs = kwargs
    #     self.session = None
    #     self.connect()

    # def connect(self):
    #     """  Establish an ADL Connection object  """
    #     token = lib.auth(tenant_id=self.tenant_id, client_id=self.client_id, 
    #         client_secret=self.client_secret)
    #     self.session = AzureDLFileSystem(token = token, store_name = self.store_name)
    #     print('connection made ...')
       
    # def _trim_filename(self, fn):
    #     """ Determine what kind of filestore this is and return the path """
    #     so = infer_storage_options(fn)
    #     files = so['path'].split("/")[1:][0]
    #     filepaths = f"{self.store_name}/{files}"
    #     print(f'trimmed filename:  {filepaths}')
    #     return filepaths

    # # def isdir(self, path):
    # #     """Is this entry directory-like?"""
    # #     try:
    # #         path = self._trim_filename(fn=path)
    # #         print(f'isdir path:  {path}')
    # #         return self.info(path)['type'] == 'directory'
    # #     except FileNotFoundError:
    # #         return False

    # def glob(self, path):
    #     """For a template path, return matching files"""
    #     print(f"adlpaths:  {path}")
    #     # allfiles = [f'{p}' for p in self.adlfs.glob(adlpaths)]
    #     print(f"fullpath:  {allfiles}")
    #     # filepaths = [f'adl://{self.store_name}.azuredatalakestore.net{p}' for p in self.adl.glob(adlpaths)]
    #     return allfiles

    # def ls(self, path):
    #     """List all objects in this directory"""
        
        

    # def info(self, path, **kwargs):
    #     """Give details of entry at path

    #     Returns a single dictionary, with exactly the same information as ``ls``
    #     would with ``detail=True``.

    #     The default implementation should calls ls and could be overridden by a
    #     shortcut. kwargs are passed on to ```ls()``.

    #     Returns
    #     -------
    #     dict with keys: name (full path in the FS), size (in bytes), type (file,
    #     directory, or something else) and other FS-specific keys.
    #     """

    #     path = self._strip_protocol(path)
    #     print(f"calling info:  {path}")
    #     print(f"calling self._parent:  {self._parent.__class__}")
    #     out = self.ls(self._parent(path), detail=True, **kwargs)
    #     print(f"This is the output:  {out}")
    #     out = [o for o in out if o['name'].rstrip('/') == path]
    #     if out:
    #         return out[0]
    #     out = self.adlfs.ls(path, detail=True, **kwargs)
    #     path = path.rstrip('/')
    #     out1 = [o for o in out if o['name'].rstrip('/') == path]
    #     if len(out1) == 1:
    #         return out1[0]
    #     elif len(out1) > 1 or out:
    #         return {'name': path, 'size': 0, 'type': 'directory'}
    #     else:
    #         raise FileNotFoundError(path)

    # def mkdirs(self, path):
    #     pass  # no need to pre-make paths on ADL

    # def open(self, path, mode='rb'):
    #     adl_path = self._trim_filename(path)
    #     f = AzureDLFileSystem.open(self, adl_path, mode=mode)
    #     return f

    # def ukey(self, path):
    #     adl_path = self._trim_filename(path)
    #     return tokenize(self.info(adl_path)['modificationTime'])

    # def size(self, path):
    #     adl_path = self._trim_filename(path)
    #     return self.info(adl_path)['length']

    # def __getstate__(self):
    #     dic = self.__dict__.copy()
    #     del dic['token']
    #     del dic['azure']
    #     logger.debug("Serialize with state: %s", dic)
    #     return dic

    # def __setstate__(self, state):
        # logger.debug("De-serialize with state: %s", state)
        # self.__dict__.update(state)
        # self.do_connect()

class AzureDatalakeFileSystem(AzureDLFileSystem):

    def __init__(self, tenant_id=None, client_id=None, client_secret=None, store_name=None,
            **kwargs):
        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = store_name
        self.kwargs = kwargs
        self.connect()

    def connect(self):
        """  Establish an ADL Connection object  """
        token = lib.auth(tenant_id=self.tenant_id, client_id=self.client_id, 
            client_secret=self.client_secret)
        self.session = AzureDLFileSystem(token = token, store_name = self.store_name)
        print('connection made ...')
        print(dir(cls))