**Change Log**
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
