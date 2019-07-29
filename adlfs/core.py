# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging
from fsspec import AbstractFileSystem
# from fsspec import AbstractBufferedFile
from azure.datalake.store import lib, AzureDLFileSystem

from fsspec.utils import infer_storage_options

logger = logging.getLogger(__name__)


class AzureDatalakeFileSystem(AbstractFileSystem):
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

    
    def __init__(self, tenant_id=None, client_id=None, client_secret=None, store_name=None,
                **kwargs):

        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = store_name
        self.connect()
        self.kwargs = kwargs

    
    def connect(self):
        """  Establish an ADL Connection object  """

        token = lib.auth(tenant_id=self.tenant_id,
                        client_id=self.client_id,
                        client_secret=self.client_secret)
        self.adl = AzureDLFileSystem(token=token, store_name=self.store_name)
       
    def _trim_filename(self, fn):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        return so['path']

    def glob(self, path):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        print(adlpaths)
        adlpaths = f'adl://{self.store_name}.azuredatalakestore.net{p}' for p in self.adl.glob(adlpaths)]
        print(adlpaths)
        # filepaths = [f'adl://{self.store_name}.azuredatalakestore.net{p}' for p in self.adl.glob(adlpaths)]
        return adlpaths

    def mkdirs(self, path):
        pass  # no need to pre-make paths on ADL

    def open(self, path, mode='rb'):
        adl_path = self._trim_filename(path)
        f = AzureDLFileSystem.open(self, adl_path, mode=mode)
        return f

    def ukey(self, path):
        adl_path = self._trim_filename(path)
        return tokenize(self.info(adl_path)['modificationTime'])

    def size(self, path):
        adl_path = self._trim_filename(path)
        return self.info(adl_path)['length']

    def __getstate__(self):
        dic = self.__dict__.copy()
        del dic['token']
        del dic['azure']
        logger.debug("Serialize with state: %s", dic)
        return dic

    def __setstate__(self, state):
        logger.debug("De-serialize with state: %s", state)
        self.__dict__.update(state)
        self.do_connect()

