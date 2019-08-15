# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging
import requests
import re


from azure.datalake.store import lib, AzureDLFileSystem
from fsspec import AbstractFileSystem
from fsspec.utils import infer_storage_options

logger = logging.getLogger(__name__)


class AzureDatalakeFileSystem(AzureDLFileSystem, AbstractFileSystem):
    
    
    """
    Access Azure Datalake Gen1 as if it were a file system.

    This exposes a filesystem-like API on top of Azure Datalake Storage

    Examples
    _________
    >>> adl = AzureDatalakeFileSystem(tenant_id="xxxx", client_id="xxxx", 
                                    client_secret="xxxx", store_name="storage_account"
                                    )
        adl.ls('')
        
        When used with Dask's read method, pass credentials as follows:
        
        dd.read_parquet("adl://folder/filename.xyz", storage_options={
            'tenant_id': TENANT_ID, 'client_id': CLIENT_ID, 
            'client_secret': CLIENT_SECRET, 'store_name': STORE_NAME,
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
        The name of the datalake account being accessed
    """

    def __init__(self, tenant_id, client_id, client_secret, store_name):
        AbstractFileSystem.__init__(self)
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = store_name
        self.do_connect()

    def do_connect(self):
        """Establish connection object."""
        token = lib.auth(tenant_id=self.tenant_id,
                        client_id=self.client_id,
                        client_secret=self.client_secret,
                        )
        AzureDLFileSystem.__init__(self, token=token,
                                   store_name=self.store_name)

    def _trim_filename(self, fn):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        fileparts = so['path']
        return fileparts

    def glob(self, path):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        filepaths = AzureDLFileSystem.glob(self, adlpaths)
        return filepaths

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
        

class AzureBlobFileSystem(AbstractFileSystem):
    
    """
    abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<file_name>

    file_system  = A container on the datalake
    account_name = The name of the storage account
    path         =  A forward slash representation of the directory structure
    file_name    = The name of an individual file in the directory
    """
    
    
    def __init__(self, tenant_id, client_id, client_secret, storage_account, 
                 filesystem, token=None):

        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.storage_account = storage_account
        self.filesystem = filesystem
        self.token = token
        self.token_type = None
        self.connect()
        self.dns_suffix = '.dfs.core.windows.net'
        root_marker = self.filesystem

    def connect(self):
        """ Fetch an OAUTh token using a ServicePrincipal """
        
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        header = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://storage.azure.com/.default",
                "grant_type": "client_credentials"}
        response = requests.post(url=url, headers=header, data=data).json()
        self.token_type=response['token_type']
        expires_in=response['expires_in']
        ext_expires_in=response['ext_expires_in']
        self.token=response['access_token']
        
    def _make_headers(self):
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'x-ms-version': '2019-02-02',
                   'Authorization': f'Bearer {self.token}'
                   }
        return headers
    
    def _parse_path(self, path: str):
        """ Extracts the name of the filesystem and the directory from the path """
        fparts = path.split('/')
        print(fparts)
        if len(fparts) == 1:
            return fparts[0], None
        else:
            return fparts[0], "/".join(fparts[1:])
    
    def _make_url(self, filesystem):
        return f"https://{self.storage_account}{self.dns_suffix}/{filesystem}"
        
    
    def ls(self, path: str, detail: bool = False, resource: str = 'filesystem', recursive: bool = False):
        """ List a single filesystem directory, with or without details
        
        Parameters
        __________
        path - string
            The Azure Datalake Gen2 filesystem name, followed by subdirectories and files
        detail - boolean
            Specified by the AbstractFileSystem.  
            If false, return a list of strings (without protocol) that detail the full path of 
        resource - string

        recursive - boolean
            Determines if the files should be listed recursively nor not.
        
        """
        try:
            path = self._strip_protocol(path)
            filesystem, directory = self._parse_path(path)
            url = self._make_url(filesystem=filesystem)
            headers = self._make_headers()
            payload = {'resource': resource,
                    'recursive': recursive}
            if directory is not None:
                payload['directory'] = directory
            response = requests.get(url=url, headers=headers, params=payload)
            # print(response.url)
            response = response.json()
            print(response)
            if response['paths']:
                pathlist = response['paths']
                if detail:
                    for path_ in pathlist:
                        if 'isDirectory' in path_.keys() and path_['isDirectory']=='true':
                            path_['type'] = 'directory'
                            del path_['isDirectory']
                        else:
                            path_['type'] = 'file'
                    return pathlist
                else:
                    files = []
                    for path_ in pathlist:
                        files.append(path_['name'])          
                    return files
            else:
                return []
        except KeyError:
            if 'error' in response.keys():
                if response['error']['code'] == 'PathNotFound':
                    return []
            else:
                raise KeyError(f'{response}')

    def info(self, path):
        """ Give details of entry at path
        
        """
        path = self.ls(path, detail=True)
        return path
    
    def glob(sefl, path):
        """ Find files by glob matching """
        
        ends = path.endswith("/")
        path = self._strip_protocol(path)
        indstar = path.find("*") if path.find("*") >= 0 else len(path)
        indques = path.find("?") if path.find("?") >= 0 else len(path)
        ind = min(indstar, indques)
        if "*" not in path and "?" not in path:
            if ends:
                # self.
                return None
        files = self.ls(path)
        

    def make_request(self, url, headers, payload):
        r = requests.get(url=url, headers=headers, params=payload)
        return r