# adlfs

`adlfs` provides an [`fsspec`][fsspec]-compatible interface to [Azure Blob storage], [Azure Data Lake Storage Gen2], and [Azure Data Lake Storage Gen1].


## Installation

`adlfs` can be installed using pip

    pip install adlfs

or conda from the conda-forge channel


    conda install -c conda-forge adlfs

## `fsspec` protocols

`adlfs` registers the following protocols with `fsspec`.

protocol | filesystem
-------- | ----------
`abfs`   | `adlfs.AzureBlobFileSystem`
`az`     | `adlfs.AzureBlobFileSystem`
`adl`    | `adlfs.AzureDatalakeFileSystem`

## Authentication

The `AzureBlobFileSystem` implementation uses the [`azure.storage.blob`] library internally. For the most
part, you can authenticate with Azure using any of the methods it supports.

For anonymous authentication, simply provide the storage account name:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest")
```

For operations to succeed, the storage container must allow anonymous access.

For authenticated access, the preferred approach is to set `anon=False` and let adlfs resolve
credentials automatically using Azure's `DefaultAzureCredential`:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest", anon=False)
```

You can also authenticate with a SAS token or account key via the `credential` argument:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest", credential=SAS_TOKEN)
>>> fs = adlfs.AzureBlobFileSystem(account_name="ai4edataeuwest", credential=ACCOUNT_KEY)
```

If you need to pass a credential object directly, use an **async** credential from
`azure.identity.aio`:

```{code-block} python
>>> from azure.identity.aio import DefaultAzureCredential
>>> fs = adlfs.AzureBlobFileSystem(
...     account_name="ai4edataeuwest",
...     credential=DefaultAzureCredential()
... )
```

Additionally, some methods will include the account URL and authentication credentials in a connection string. To use this, provide just `connection_string`:

```{code-block} python
>>> fs = adlfs.AzureBlobFileSystem(connection_string=CONNECTION_STRING)
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
[`azure.storage.blob`]: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
[fsspec documentation]: https://filesystem-spec.readthedocs.io/en/latest/usage.html
