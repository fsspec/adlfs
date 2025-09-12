import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal

from adlfs import AzureBlobFileSystem
from adlfs.tests.constants import CONN_STR


def test_dask_parquet(storage):
    fs = AzureBlobFileSystem(
        account_name=storage.account_name, connection_string=CONN_STR
    )
    fs.mkdir("test")
    STORAGE_OPTIONS = {
        "account_name": "devstoreaccount1",
        "connection_string": CONN_STR,
    }
    df = pd.DataFrame(
        {
            "col1": [1, 2, 3, 4],
            "col2": [2, 4, 6, 8],
            "index_key": [1, 1, 2, 2],
            "partition_key": [1, 1, 2, 2],
        }
    )

    dask_dataframe = dd.from_pandas(df, npartitions=1)
    for protocol in ["abfs", "az"]:
        dask_dataframe.to_parquet(
            "{}://test@dfs.core.windows.net/test_group.parquet".format(protocol),
            storage_options=STORAGE_OPTIONS,
            engine="pyarrow",
            write_metadata_file=True,
        )

        fs = AzureBlobFileSystem(**STORAGE_OPTIONS)
        assert fs.ls("test/test_group.parquet") == [
            "test/test_group.parquet/_common_metadata",
            "test/test_group.parquet/_metadata",
            "test/test_group.parquet/part.0.parquet",
        ]
        fs.rm("test/test_group.parquet")

    df_test = dd.read_parquet(
        "abfs://test/test_group.parquet",
        storage_options=STORAGE_OPTIONS,
        engine="pyarrow",
    ).compute()
    assert_frame_equal(df, df_test)


def test_account_name_from_url():
    kwargs = AzureBlobFileSystem._get_kwargs_from_urls(
        "abfs://test@some_account_name.dfs.core.windows.net/some_file"
    )
    assert kwargs["account_name"] == "some_account_name"


def test_azure_storage_url_routing():
    """Test that AzureBlobFileSystem correctly handles Azure Storage URLs"""
    
    # Test various Azure Storage URL formats
    azure_urls_and_expected = [
        ("abfss://container@account.dfs.core.windows.net/file", "account"),
        ("abfs://container@account.dfs.core.windows.net/file", "account"),
        ("abfss://container@account.blob.core.windows.net/file", "account"),
        ("az://container@account.blob.core.windows.net/file", "account"),
    ]
    
    for url, expected_account in azure_urls_and_expected:
        kwargs = AzureBlobFileSystem._get_kwargs_from_urls(url)
        assert kwargs.get("account_name") == expected_account, f"Failed for URL: {url}"


def test_onelake_url_ignored_by_azure_blob_fs():
    """Test that AzureBlobFileSystem ignores OneLake URLs"""
    
    # OneLake URLs should be ignored by AzureBlobFileSystem
    onelake_urls = [
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/file",
        "abfs://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/file",
    ]
    
    for url in onelake_urls:
        kwargs = AzureBlobFileSystem._get_kwargs_from_urls(url)
        # Should return empty dict (no account_name extracted)
        assert kwargs == {}, f"AzureBlobFileSystem should ignore OneLake URL: {url}"


def test_azure_vs_onelake_domain_routing():
    """Test that domain-based routing works correctly"""
    
    # Azure Storage domains should be handled by AzureBlobFileSystem
    azure_domains = [
        "abfss://container@account.dfs.core.windows.net/file",
        "abfss://container@account.blob.core.windows.net/file",
    ]
    
    for url in azure_domains:
        kwargs = AzureBlobFileSystem._get_kwargs_from_urls(url)
        assert kwargs.get("account_name") == "account", f"Azure domain not handled correctly: {url}"
    
    # OneLake domains should be ignored by AzureBlobFileSystem
    onelake_domains = [
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/file",
    ]
    
    for url in onelake_domains:
        kwargs = AzureBlobFileSystem._get_kwargs_from_urls(url)
        assert kwargs == {}, f"OneLake domain should be ignored: {url}"
