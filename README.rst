Dask interface to Azure-Datalake Gen1 and Gen2 Storage
------------------------------------------------------

Warning: this code is experimental and untested.

Quickstart
----------
This package is on PyPi and can be installed using:

pip install adlfs

In your code, call:
    from fsspec.registry import known_implementations

To use the Gen1 filesystem:
    known_implementations['adl'] = {'class': 'adlfs.AzureDatalakeFileSystem'}

To use the Gen2 filesystem:
    known_implementations['abfs'] = {'class': 'adlfs.AzureBlobFileSystem'}

This allows operations such as:
import dask.dataframe as dd
storage_options={
'tenant_id': TENANT_ID,
'client_id': CLIENT_ID,
'client_secret': CLIENT_SECRET,
'storage_account': STORAGE_ACCOUNT,
'filesystem': FILESYSTEM,
}
dd.read_csv('abfs://folder/file.csv', storage_options=STORAGE_OPTIONS}

Details
-------
The package includes pythonic filesystem implementations for both 
Azure Datalake Gen1 and Azure Datalake Gen2, that facilitate 
interactions with both Azure Datalake implementations with Dask, using the 
intake/filesystem_spec base class.

Operations against both Gen1 and Gen2 datalakes currently require an Azure ServicePrincipal
with suitable credentials to perform operations on the resources of choice.

Operations on the Azure Gen1 Datalake are implemented by leveraging multiple inheritance from both
the fsspec.AbstractFileSystem and the Azure Python Gen1 Filesystem library, while
operations against the Azure Gen2 Datalake are implemented by using subclassing the 
fsspec.AbstractFileSystem and leveraging the Azure Datalake Gen2 API.  Note that the Azure 
Datalake Gen2 API allows calls to using either the 'http://' or 'https://' protocols, designated
by an 'abfs[s]://' protocol.  Under the hood in adlfs, this will always happen using 'https://'
using the requests library.

An Azure Datalake Gen2 url takes the following form, which is replicated in 
the adlfs library, for the sake of consistency:
'abfs[s]://{storage_account}/{filesystem}/{folder}/{file}'

Currently, when using either the 'adl://' or 'abfs://' protocols in a dask operation, 
it is required to explicitly declare the storage_options, as described in the Dask documentation.
The intent is to eliminate this requirement for (at at minimum) Gen2 operations, by having 
the adlfs library parse the filesystem name