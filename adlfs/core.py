# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging
import requests
import re


from azure.datalake.store import lib, AzureDLFileSystem
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, stringify_path

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
    
    def isdir(self, path):
        """Is this entry directory-like?"""
        try:
            return self.info(path)['type'].lower() == 'directory'
        except FileNotFoundError:
            return False

    def isfile(self, path):
        """Is this entry file-like?"""
        try:
            return self.info(path)['type'].lower() == 'file'
        except:
            return False

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
    """

    protocol = 'abfs'

    def __init__(self, tenant_id: str, client_id: str, client_secret: str,
                 storage_account: str, filesystem: str, token=None):

        """
        Parameters
        ----------
        tenant_id: Azure tenant
        client_id: Azure ServicePrincipal
        client_secret: Azure ServicePrincipal secret (password)
        storage_account: Name of the Azure Datalake Gen2 account
        file_system: A container (buckeet) on the datalake
        token: Azure security token acquired to authorize request
        """

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

    @classmethod
    def _strip_protocol(cls, path):
        """ Turn path from fully-qualified to file-system-specific

        May require FS-specific handling, e.g., for relative paths or links.
        """
        path = stringify_path(path)
        protos = (cls.protocol, ) if isinstance(
            cls.protocol, str) else cls.protocol
        for protocol in protos:
            path = path.rstrip('/')
            if path.startswith(protocol):
                protocol_ = path.split('://')[0]
                path = path[len(protocol_) + 3:]
        # use of root_marker to make minimum required path, e.g., "/"
        return path or cls.root_marker

    def connect(self):
        """ Fetch an OAUTh token using a ServicePrincipal """
        
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        header = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://storage.azure.com/.default",
                "grant_type": "client_credentials"}
        response = requests.post(url=url, headers=header, data=data).json()
        self.token_type = response['token_type']
        expires_in = response['expires_in']
        ext_expires_in = response['ext_expires_in']
        self.token = response['access_token']

    def _make_headers(self, range: str = None, encoding: str = None):
        """ Creates the headers for an API request to Azure Datalake Gen2
        
        parameters
        ----------
        range: String that specifies the byte ranges.  Used by the buffered file
        encoding: String that specifies content-encoding applied to file.  Maps
            to API request header "Content-Encoding"
        """
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'x-ms-version': '2019-02-02',
                   'Authorization': f'Bearer {self.token}'
                   }
        if range:
            headers['Range'] = str(range)
        return headers

    def _parse_path(self, path: str):
        """ Extracts the directory, subdirectories, and files from the path """
        fparts = path.split('/')
        if len(fparts) == 0:
            return []
        else:
            return "/".join(fparts)

<<<<<<< Updated upstream
    def _make_url(self, path: str =None):
        """ Creates a url for making a request to the Azure Datalake Gen2 API """
        if not path:
            return f"https://{self.storage_account}{self.dns_suffix}/{self.filesystem}"
        else: return f"https://{self.storage_account}{self.dns_suffix}/{self.filesystem}/{path}"
=======
    def _make_url(self, resource: str = None):
        """ Creates a url for making a request to the Azure Datalake Gen2 API """
        return f"https://{self.storage_account}{self.dns_suffix}/{self.filesystem}"
>>>>>>> Stashed changes

    def ls(self, path: str, detail: bool = False, resource: str = 'filesystem',
           recursive: bool = False):
        """ List a single filesystem directory, with or without details
        
        Parameters
        __________
        path: The Azure Datalake Gen2 filesystem name, followed by subdirectories and files
        detail: Specified by the AbstractFileSystem.  If false, return a list of strings (without protocol) that detail the full path of 
        resource: Variable to be passed to the Microsoft API
        recursive:  Determines if the files should be listed recursively nor not.
        """
        
        try:
            path = self._strip_protocol(path)
            directory = self._parse_path(path)
            url = self._make_url()
            headers = self._make_headers()
            payload = {'resource': resource,
                       'recursive': recursive
                       }
            if directory is not None:
                payload['directory'] = directory
            response = requests.get(url=url, headers=headers, params=payload)
            if not response.status_code == requests.codes.ok:
                response.raise_for_status()
            response = response.json()
            if response['paths']:
                pathlist = response['paths']
                if detail:
                    for path_ in pathlist:
                        if 'isDirectory' in path_.keys() and path_['isDirectory']=='true':
                            # fsspec expects the api call to include a key named "type", 
                            # but Azure returns a key 'isDirectory' to specify if the 
                            # item is a directory vs file, hence the update.
                            path_['type'] = 'directory'
                        else:
                            # Azure uses a different set of keys in the API response, 
                            # such that, an object is assumed to be a file unless it 
                            # contains the above dictionary key.
                            path_['type'] = 'file'
                        # Finally, fsspec expects the API response to return a key 'size'
                        # that specifies the size of the file in bytes, but the Azure DL
                        # Gen2 API returns the key 'contentLength'.  We update this below.
                        if 'contentLength' in path_.keys():
                            path_['size'] = int(path_.pop('contentLength'))
                        else: path_['size'] = int(0)
                    if len(pathlist) == 1:
                        return pathlist[0]
                    else:
                        print(pathlist)
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

    def info(self, path: str = '', detail=True):
        """ Give details of entry at path"""
        path = self._strip_protocol(path)
        print('info..')
        print(path)
        url = self._make_url(path=path)
        print(url)
        headers = self._make_headers()
        payload = {'action': 'getStatus'}
        response = requests.head(url=url, headers=headers, params=payload)
        print(response.url)
        if not response.status_code == requests.codes.ok:
            try:
                detail = self.ls(path, detail=False)
                return detail
            except:
                response.raise_for_status()
        h = response.headers
        detail = {'name': path,
                'size': int(h['Content-Length']),
                'type': h['x-ms-resource-type']
                }
        return detail


    def _open(self, path, mode='rb', block_size=None, autocommit=True):
       """ Return a file-like object from the ADL Gen2 in raw bytes-mode """
       
       return AzureBlobFile(self, path, mode)

    # def glob(self, path):
    #     raise NotImplementedError

class AzureBlobFile(AbstractBufferedFile):
    """ Buffered Azure Datalake Gen2 File Object """

<<<<<<< Updated upstream
    def __init__(self, fs, path, mode='rb', blocksize='default',
=======
    def __init__(self, fs, path, mode='rb', block_size='default',
>>>>>>> Stashed changes
                 cache_type='bytes', autocommit=True):
        super().__init__(fs, path, mode, blocksize=blocksize,
                    cache_type=cache_type, autocommit=autocommit)
        self.fs = fs
        self.path = path
<<<<<<< Updated upstream
=======
        self.cache = b''
        self.closed = False

    # def read(self, length=-1):
    #     """Read bytes from file"""

    #     if (
    #         (length < 0 and self.loc == 0) or
    #         (length > (self.size or length)) or
    #         (self.size and self.size < self.block_size)
    #     ):
    #         self._fetch_all()
    #     if self.size is None:
    #         if length < 0:
    #             self._fetch_all()
    #     else:
    #         length = min(self.size - self.loc, length)
    #         return super().read(length)

    def _fetch_all(self):
        """Read the whole file in one show.  Without caching"""
        
        headers = self.fs._make_headers(range=(0, self.size))
        url = f'{self.fs._make_url()}/{self.path}'
        response = requests.get(url=url, headers=headers)
        data = response.content
        self.cache = AllBytes(data)
        self.size = len(data)
>>>>>>> Stashed changes

    def _fetch_range(self, start=None, end=None):
        """ Gets the specified byte range from Azure Datalake Gen2 """
        print('?????? _fetch_range ??????')
        print(start, end)
        if start is not None or end is not None:
            start = start or 0
            end = end or 0
            print(f'start is:  {start}, end is:  {end}')
            headers = self.fs._make_headers(range=(start, end-1))
        else:
<<<<<<< Updated upstream
            headers = self.fs._make_headers(range=(None))

=======
            headers = self.fs._make_headers(range=None)

        headers = self.fs._make_headers(range=(start, end))
>>>>>>> Stashed changes
        url = f'{self.fs._make_url()}/{self.path}'
        response = requests.get(url=url, headers=headers)
        data = response.content
        print(f'This is data from _fetch_range:  {data}')
        return data

    def _upload_chunk(self, final: bool = False, resource: str = None):
        """ Writes part of a multi-block file to Azure Datalake """
        headers = self.fs._make_headers()
        headers['Content-Length'] = '0'
        url = self.fs._make_url()
        url = f'{url}/{self.path}'
        params = {'resource': 'file'}
        response = requests.put(url, headers=headers, data=self.buffer, params=params)
        if not response.status_code == requests.codes.ok:
            response.raise_for_status()


<<<<<<< Updated upstream

   
=======
class AllBytes:
    """ Cache the entire contents of a remote file """
    def __init__(self, data):
        self.data = data

    def _fetch(self, start, end):
        return self.data[start:end]
>>>>>>> Stashed changes
