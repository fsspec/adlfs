**Change Log**
v0.7.0
------
- Updated path structure, so virtual folders in Azure blobs are created with a
  trailing "/", to fix #137
- Converted all references to blob_clients to occur inside a context manager
- Added metadata to blob reads and writes, with {"is_directory": "<true or false>"}
  to help resolve ambiguous folder and file discrepancies
- Minor update to assure compatibility with fsspec==0.8.7
- Added error handling to AzureBlobFile finalizer
- Updated AzureBlobFile.close method to execute threadsafe with asyncio
- Added check for leading "?" in sas_token parameter
- Fix for setting loop attribute on newer version of fsspec [#197](https://github.com/dask/adlfs/issues/197)
- Converted self.mkdir() to a no-op to align to fsspec issue [#562](https://github.com/intake/filesystem_spec/issues/562)
- Fixed errors with pyarrow write_dataset method related to pseudo-directories in [*171](https://github.com/dask/adlfs/issues/171).
- Fixed race conditions with finalizer on AzureBlobFile object

v0.6.3
------
- Added ability to connect with only connection_string.
- connection_string now pre-empts other credentials.

v0.6.2
------
- Added ability to fetch multiple AZURE_STORAGE_XYZ credentials as environmental variables


v0.6.1
------
- Removed weakref and added close method to fix memory leak
- Added **kwargs reference to _cp_file

v0.6.0
------
- Implemented asynchronous uploads and downloads of AzureBlobFile objects
- Aligned on policy for asynchronous context managers to be preferred with blob_client.
- Added abfs.makedir() method and aligned exist_ok parameter to fsspec api 
- Updated abfs.mkdir() method to align to fsspec api.
- Pinned azure-storage-blob >= 12.5.0,<12.7.0 -- version 12.7.0 breaks compatability with azurite for unit testing


v0.5.9
------

- Fixed expception raised when commiting writes of large blobs
- Improved performance when checking for the existance of a key in a container with `.exists()`
- Improved performance of `.find()` 
- Improved documentation for Azure SDK
- Fixed failed authentication when passing a credential


> v0.3.0
-----
- Updated azure-storage-blob dependency to >=v12
- Addede azure-core as dependency for exception handling
- Updated code for compatibility with fsspec > 0.6
