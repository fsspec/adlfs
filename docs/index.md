# adlfs

`adlfs` provides an [`fsspec`][fsspec]-compatible interface to [Azure Blob storage], [Azure Data Lake Storage Gen2], [Azure Data Lake Storage Gen1], and [Microsoft OneLake].


## Installation

`adlfs` can be installed using pip

    pip install adlfs

or conda from the conda-forge channel


    conda install -c conda-forge adlfs

## Microsoft Storage Ecosystem Overview

Microsoft provides several data storage solutions for different use cases:

### Azure Data Lake Storage Gen1 (ADLS Gen1)
- **Status**: Legacy, being retired
- **Type**: Hierarchical file system, POSIX compliant
- **Endpoint**: `https://<account>.azuredatalakestore.net`
- **Use Case**: Legacy big data workloads

### Azure Data Lake Storage Gen2 (ADLS Gen2) 
- **Status**: Current recommended solution
- **Type**: Based on Blob storage with hierarchical namespace
- **Endpoints**: 
  - Blob Service: `https://<account>.blob.core.windows.net`
  - Data Lake Service: `https://<account>.dfs.core.windows.net`
- **Use Case**: Modern data lake and analytics workloads

### Microsoft OneLake
- **Status**: Newest, part of Microsoft Fabric platform
- **Type**: Unified data lake with Delta Lake format, ACID transactions
- **Endpoint**: `https://onelake.dfs.fabric.microsoft.com`
- **Use Case**: Microsoft Fabric analytics platform

### OneDrive/SharePoint
- **Note**: For OneDrive, Teams files, and SharePoint document libraries, use [`msgraphfs`](https://github.com/acsone/msgraphfs) instead of `adlfs`

## `fsspec` protocols

`adlfs` registers the following protocols with `fsspec`.

protocol | filesystem | storage type
-------- | ---------- | ------------
`abfs`   | `adlfs.AzureBlobFileSystem` | Azure Blob Storage / ADLS Gen2
`abfss`  | `adlfs.AzureBlobFileSystem` or `adlfs.OneLakeFileSystem`* | Azure Blob Storage / ADLS Gen2 / OneLake
`az`     | `adlfs.AzureBlobFileSystem` | Azure Blob Storage / ADLS Gen2
`adl`    | `adlfs.AzureDatalakeFileSystem` | Azure Data Lake Storage Gen1
`onelake`| `adlfs.OneLakeFileSystem` | Microsoft OneLake

*`abfss` URLs are automatically routed to the correct filesystem based on the domain:
- `*.dfs.core.windows.net` → `AzureBlobFileSystem` 
- `onelake.dfs.fabric.microsoft.com` → `OneLakeFileSystem`

## Authentication

The `AzureBlobFileSystem` implementation uses the [`azure.storage.blob`] library internally. For the most
part, you can authenticate with Azure using any of the methods it supports.

For anonymous authentication, simply provide the storage account name:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest")
```

For operations to succeed, the storage container must allow anonymous access.

For authenticated access, you have several options:

1. Using a `SAS_TOKEN`
2. Using an account key
3. Using a managed identity

Regardless of the method your using, you provide the values using the `credential` argument.

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest", credential=SAS_TOKEN)
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest", credential=ACCOUNT_KEY)
>>> fs = adlfs.AzureBlobFileSystem(
...     account_name="ai4edataeuwest",
...     credential=azure.identity.DefaultAzureCredential()
... )
```

Additionally, some methods will include the account URL and authentication credentials in a connection string. To use this, provide just `connection_string`:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(connection_string=CONNECTION_STRING)
```

### OneLake Authentication

OneLake requires Azure Active Directory authentication. You can authenticate using:

```{code-block} python
>>> from adlfs import OneLakeFileSystem
>>> fs = OneLakeFileSystem(
...     tenant_id="your-tenant-id",
...     client_id="your-client-id", 
...     client_secret="your-client-secret"
... )
```

Or using environment variables:

```{code-block} python
>>> import os
>>> os.environ["AZURE_TENANT_ID"] = "your-tenant-id"
>>> os.environ["AZURE_CLIENT_ID"] = "your-client-id"
>>> os.environ["AZURE_CLIENT_SECRET"] = "your-client-secret"
>>> fs = OneLakeFileSystem()
```

## Usage

See the [fsspec documentation] on usage.

Note that `adlfs` generally uses just the "name" portion of an account name. For example, you would provide
`account_name="ai4edataeuwest"` rather than `account_name="https://ai4edataeuwest.blob.core.windows.net"`.

When working with Azure Blob Storage, the *container name* is included in path operations. For example,
to list all the files or directories in the top-level of a storage container, you would call `fs.ls("<container_name>")`:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest")
>>> fs.ls("gbif")
['gbif/occurrence']
```

### OneLake Usage

OneLake paths follow the structure `workspace/lakehouse/path/to/file`. You can use both `onelake://` and `abfss://` protocols:

```{code-block} python
>>> from adlfs import OneLakeFileSystem
>>> fs = OneLakeFileSystem(tenant_id="...", client_id="...", client_secret="...")

# List contents of a lakehouse
>>> fs.ls("my_workspace/my_lakehouse") 
['my_workspace/my_lakehouse/Files', 'my_workspace/my_lakehouse/Tables']

# Using with fsspec
>>> import fsspec
>>> with fsspec.open("onelake://my_workspace/my_lakehouse/Files/data.parquet") as f:
...     data = f.read()

# Using abfss protocol (automatically routes to OneLake)
>>> with fsspec.open("abfss://my_workspace@onelake.dfs.fabric.microsoft.com/my_lakehouse/Files/data.parquet") as f:
...     data = f.read()
```

```{toctree}
:maxdepth: 2

api.md
```

**Note**: When uploading a blob (with `write_bytes` or `write_text`) you can injects kwargs directly into `upload_blob` method:

```{code-block} python
>>> from azure.storage.blob import ContentSettings
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest")
>>> fs.write_bytes(path="path", value=data, overwrite=True, **{"content_settings": ContentSettings(content_type="application/json", content_encoding="br")})
```

[fsspec]: https://filesystem-spec.readthedocs.io
[Azure Blob storage]: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
[Azure Data Lake Storage Gen2]: https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction
[Azure Data Lake Storage Gen1]: https://docs.microsoft.com/en-us/azure/data-lake-store/
[Microsoft OneLake]: https://docs.microsoft.com/en-us/fabric/onelake/
[`azure.storage.blob`]: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
[fsspec documentation]: https://filesystem-spec.readthedocs.io/en/latest/usage.html
