**Change Log**

Unreleased
----------

- Handle mixed casing for `hdi_isfolder` metadata when determining whether a blob should be treated as a folder.
- `_put_file`: `overwrite` now defaults to `True`.

2023.10.0
---------
- Added support for timeout/connection_timeout/read_timeout

2023.9.0
---------
- Compatability with new ``glob`` behavior in fsspec 2023.9.0
- Compatability with ``azure.storage.blob`` 12.18.1

2022.11.2
---------
- Reorder fs.info() to search the parent directory only after searching for the specified item directly
- Removed pin on upper bound for azure-storage-blob
- Moved AzureDatalakeFileSystem to a separate module, in acknowledgement of [Microsoft End of Life notice](https://learn.microsoft.com/en-us/answers/questions/281107/azure-data-lake-storage-gen1-retirement-announceme.html})
- Added DeprecationWarning to AzureDatalakeFilesystem


2022.10.1
----------
- Pin azure-storage-blob >=12.12.0,<12.14.  Requires pinning azure-core>=1.23.1,<2.0.0

2022.9.1
--------
- Fixed missing dependency on `aiohttp` in package metadata.

2022.9.0
--------
- Add support to AzureBlobFileSystem for versioning
- Assure full uri's are left stripped to remove "\"
- Set Python requires >=3.8 in setup.cfg
- _strip_protocol handle lists

2022.7.0
--------
- Fix overflow error when uploading files > 2GB

2022.04.0
---------
- Added support for Python 3.10 and pinned Python 3.8
- Skip test_url due to bug in Azurite
- Added isort and update pre-commit

v2022.02.0
----------
- Updated requirements to fsspec >= 2021.10.1 to fix #280
- Fixed deprecation warning in pytest_asyncio by setting asycio_mode = True

v2021.10.1
----------
- Added support for Hierarchical Namespaces in Gen2 to enable multilevel Hive partitioned tables in pyarrow
- Registered abfss:// as an entrypoint instead of registering at runtime
- Implemented support for fsspec callbacks in put_file and get_file

v2021.09.1
----------
- Fixed isdir() bug causing some directories to be labeled incorrectly as files
- Added flexible url handling to improve compatibility with other applications using Spark and fsspec

v2021.08.1
----------
- Fixed call to isdir(), to run direct call to Azure container, instead of calling .ls on the directory
- Fixed call to isfile() to directly evaluate the file, insteading of calling ls on the directory
- Updated unit test for isdir
- Added DefaultAzureCredential as an authentication method.  Activated if anon is False, and
  no explicit credentials are passed.  See authentication methods enabled [here](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python). 
- Updated initiate_upload() method to allow overwriting a blob if the credentials do not have delete priveleges.  

v0.7.7
------
- Fixed bug in fetch_range that caused attempted reads beyond end of file
- Added check to mkdir when creating a container to confirm container_name conforms
  to Azure requirements

v0.7.6
------

- Compatability with fsspec 2021.6.0
- `exists()` calls now also checks whether a directory with that name exists or not. Previously this was only checked from the cache
- Fixed bug in `find` not returning exact matches
- Added `AzureDatalakeFileSystem.rm_file` and `AzureDatalakeFileSystem.rmdir`
- Fixed bug in `filter_blobs` when target path contained a special character

v0.7.4
------
- Added the location_mode parameter to AzureBlobFileSystem object, and set default to "primary" to enable Access Control Lists and RA-GRS access.  Valid values are "primary" and "secondary"


v0.7.2
-----
- Now can create / destroy containers with `mkdir()`/`rmdir()` without needing to
have the privilige of listing all containers.
- When called with only container name, `exists()` now returns the proper result
(about the existence of the container) instead of a hard-coded `True`
- Removed version constraints on azure-storage-blob, pytest, and docker
- Updated conftest script to start Azurite docker container

v0.7.1
------
- fsspec version updated to 0.9.0

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
- Pinned fsspec to 0.8.7 to respect this change in async

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
