# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import

import logging
import requests
import re


from azure.datalake.store import lib, AzureDLFileSystem
from azure.datalake.store.core import AzureDLPath
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options, stringify_path, tokenize
import numpy as np

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
        self.fs = AzureDLFileSystem(token=token,
                                   store_name=self.store_name)
        
    def ls(self, path, detail=False, invalidate_cache=True):
        return self.fs.ls(path=path, detail=detail,
                          invalidate_cache=invalidate_cache)
    
    def info(self, path, invalidate_cache=True):
        info = self.fs.info(path=path, invalidate_cache=invalidate_cache)
        info['size'] = info['length']
        return info

    def _trim_filename(self, fn):
        """ Determine what kind of filestore this is and return the path """
        so = infer_storage_options(fn)
        fileparts = so['path']
        return fileparts

    def glob(self, path):
        """For a template path, return matching files"""
        adlpaths = self._trim_filename(path)
        filepaths = self.fs.glob(adlpaths)
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
            return self.fs.info(path)['type'].lower() == 'file'
        except:
            return False

    def open(self, path, mode='rb'):
        f = self.fs.open(path, mode=mode)
        return f

    def ukey(self, path):
        return tokenize(self.info(path)['modificationTime'])

    def size(self, path):
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

    def _make_headers(self, content_length=None, content_type: str = None, media_type: str = None, range: str = None, 
                      encoding: str = None, **kwargs):
        """ Creates the headers for an API request to Azure Datalake Gen2
        
        parameters
        ----------
        content_type: String
        media_type: String that specified media type to be uploaded
        range: String that specifies the byte ranges.  Used by the buffered file
        encoding: String that specifies content-encoding applied to file.  Maps
            to API request header "Content-Encoding"
        content_length: Can be bassed as one of string of int64
        """
        headers = {
            # 'Content-Type': 'application/x-www-form-urlencoded',
            'x-ms-version': '2019-02-02',
            'Authorization': f'Bearer {self.token}'
                }
        if content_type:
            headers['Content-Type'] = content_type
        if media_type:
            headers['Media-Type'] = media_type
        if range:
            headers['Range'] = str(range)
        if content_length is not None:
            headers['Content-Length'] = content_length
        for k, v in kwargs.items():
            headers[k] = v
        return headers

    def _parse_path(self, path: str):
        """ Extracts the directory, subdirectories, and files from the path """
        fparts = path.split('/')
        if len(fparts) == 0:
            return []
        else:
            return "/".join(fparts)

    def _make_url(self, path: str =None):
        """ Creates a url for making a request to the Azure Datalake Gen2 API """
        if not path:
            return f"https://{self.storage_account}{self.dns_suffix}/{self.filesystem}"
        else: return f"https://{self.storage_account}{self.dns_suffix}/{self.filesystem}/{path}"

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
            headers = self._make_headers(content_type='application/x-www-form-urlencoded')
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
                        return pathlist
                        # return pathlist[0]
                    else:
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
        url = self._make_url(path=path)
        headers = self._make_headers(content_type='application/x-www-form-urlencoded')
        payload = {'action': 'getStatus'}
        response = requests.head(url=url, headers=headers, params=payload)
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


class AzureBlobFile(AbstractBufferedFile):
    """ Buffered Azure Datalake Gen2 File Object """

    def __init__(self, fs, path, mode='rb', blocksize='default',
                 cache_type='bytes', autocommit=True):
        super().__init__(fs, path, mode, blocksize=blocksize,
                    cache_type=cache_type, autocommit=autocommit)
        self.fs = fs
        self.path = path
        
    def _fetch_range(self, start=None, end=None):
        """ Gets the specified byte range from Azure Datalake Gen2 """
        if start is not None or end is not None:
            start = start or 0
            end = end or 0
            headers = self.fs._make_headers(content_type=
                                            'application/x-www-form-urlencoded',
                                            range=(start, end-1))
        else:
            headers = self.fs._make_headers(content_type=
                                            'application/x-www-form-urlencoded',
                                            range=(None))

        url = f'{self.fs._make_url()}/{self.path}'
        response = requests.get(url=url, headers=headers)
        data = response.content
        return data

    def _initiate_upload(self):
        """ Creates a file that can be written """
        headers = self.fs._make_headers(media_type='application/octet-stream', 
                                        content_length='0',
                                        content_type='application/x-www-form-urlencoded',
                                        )
        url = self.fs._make_url(path=self.path)
        params = {'resource': 'file'}
        response = requests.put(url, headers=headers, data=self.buffer, params=params)
        if not response.status_code == requests.codes.ok:
            response.raise_for_status()

    def _get_size(self):
        content_length = self.fs.info(path=self.path)['size']
        return content_length

    def _flush_file(self, position):
        url = self.fs._make_url(path=self.path)
        params = {'action': 'flush',
                  'position': position
                  }
        headers = self.fs._make_headers(media_type='text/plain',
                                            content_length=0
                                            )
        response = requests.patch(url, headers=headers, params=params)
        if not response.status_code == requests.codes.ok:
            response.raise_for_status()
        return response.status_code
        
    def _append_file(self, position, content_length, data):
        url = self.fs._make_url(path=self.path)
        headers = self.fs._make_headers(media_type='text/plain',
                                   content_length=content_length,
                                   )
        params = {'action': 'append',
                'position': self.offset
                }
        response = requests.patch(url, headers=headers, params=params,
                                  data=data)
        if not response.status_code == requests.codes.ok:
            response.raise_for_status()
        return response.status_code

    def _upload_chunk(self, final: bool = False, resource: str = None):
        """ Writes part of a multi-block file to Azure Datalake """
        self.buffer.seek(0)
        data = self.buffer.getvalue()
        l = len(data)
        end_position = self.offset + l
        
        if final and l == 0:
            status_code = self._flush_file(position=end_position)
            if status_code!=202:
                raise Exception(f'Flush to Azure Datalake failed with status code={status_code} on file {self.path}')
        
        elif final and l > 0:
        # Append current buffer to the existing file
            status_code = self._append_file(position=self.offset, content_length=l, data=data)
            if status_code==202:
                status_code = self._flush_file(position=end_position)
                if status_code != 200:
                    raise Exception(f'Flush to Azure Datalake failed with status code={status_code} on file {self.path}')
                
        elif not final and l > 0:
            status_code = self._append_file(position=self.offset, content_length=l, data=data)
            if status_code != 202:
                raise Exception(f'Write to Azure Datalake failed with status code={status_code} on file {self.path}')
                
        else:
            raise Exception(f'Unexpected condition during _upload_chunk for l={l}, final={final}, end_position={end_position}, path={self.path}')
            

    def upload_single_shot(self, final: bool = False):
        """ Writes an entire file to Azure Datalake """
        headers = self.fs._make_headers()
        headers['Content-Length'] = '0'
        url = self.fs._make_url(path=self.path)
        params = {'resource': 'file'}
        response = requests.put(url, headers=headers, data=self.buffer, params=params)
        if not response.status_code == requests.codes.ok:
            response.raise_for_status()