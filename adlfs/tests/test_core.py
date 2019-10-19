from unittest import mock
import pytest
import requests
import responses
from dask.bytes.core import read_bytes

from adlfs.core import AzureBlobFileSystem, AzureBlobFile

## Tests against AzureBlobFileSystem
        
class MockBlockBlobService:
    @staticmethod
    def get_blob_properties(container_name, blob_name):
        blob = TestBlob()
        return blob

# Fixture that sets up the AzureBlobFileSystem for calls
@pytest.fixture()
def test_mock_abfs(monkeypatch):
    tenant_id = 'test_tenant'
    client_id = 'test_client'
    client_secret = 'client_secret'
    storage_account = 'test_storage_account'
    filesystem = 'test_filesystem'
    account_name = 'test_account'
    account_key = 'test_key'
    container_name = 'test_container'
    
    def mock_
    
    def mock_do_connect(conn):
        conn.blob_fs = MockBlockBlobService()
        return conn
    
    monkeypatch.setattr(AzureBlobFileSystem, "do_connect", mock_do_connect)
    
    mock_fs = AzureBlobFileSystem(account_name=account_name, account_key=account_key,
                                  container_name=container_name
                                )
    return mock_fs


def test__strip_protocol(test_mock_abfs):
    fs = test_mock_abfs
    path

def test_make_url(test_mock_abfs):
    fs = test_mock_abfs
    url = fs._make_url()
    assert url == "https://test_storage_account.dfs.core.windows.net/test_filesystem"

@responses.activate
def test_ls(test_mock_abfs):
    responses.add(responses.GET, 'https://test_storage_account.dfs.core.windows.net/test_filesystem', 
                       json={'paths': [
                {'name': 'subfolder', 'isDirectory': 'true', 'contentLength': '5'},
                {'name': 'testfile.csv', 'contentLength': '18'},
                {'name': 'testfile2.csv', 'contentLength': '20'}
                    ]
                })
    
    """ Test that ls returns a list of approopriate files """
    # Get the mock filesystem
    fs = test_mock_abfs
    files = fs.ls("")
    assert files == ['subfolder', 'testfile.csv', 'testfile2.csv']

@responses.activate
def test_ls_nested(test_mock_abfs):
    """ Verify a ls call to a nested directory returns the correct response """
    responses.add(responses.GET, 
                  'https://test_storage_account.dfs.core.windows.net/test_filesystem?resource=filesystem&recursive=False&directory=subfolder%2Fnested_subfolder',
                  json={'paths': [{'name': 'subfolder/nested_subfolder/testfile.csv'},
                                  {'name': 'subfolder/nested_subfolder/writefile.csv'}]
                        }
                  )
    
    mock_fs = test_mock_abfs
    files = mock_fs.ls("abfs://subfolder/nested_subfolder")
    assert files == ['subfolder/nested_subfolder/testfile.csv',
                     'subfolder/nested_subfolder/writefile.csv']

@responses.activate    
def test_ls_detail(test_mock_abfs):
    """ Verify a directory can be found when requested """
    
    responses.add(responses.GET, 'https://test_storage_account.dfs.core.windows.net/test_filesystem',
                  json={'paths': [
            {'name': 'subfolder', 'isDirectory': 'true', 'contentLength': '5'},
            {'name': 'testfile.csv', 'contentLength': '18'},
            {'name': 'testfile2.csv', 'contentLength': '20'}
            ]
        })
    fs = test_mock_abfs
    
    files = fs.ls(path="", detail=True)
    assert files == [
        {'name': 'subfolder', 'type': 'directory', 'isDirectory': 'true', 'size': 5},
        {'name': 'testfile.csv', 'type': 'file', 'size': 18},
        {'name': 'testfile2.csv', 'type': 'file', 'size': 20}
        ]

@responses.activate
def test_info(test_mock_abfs):
    responses.add(responses.HEAD, 'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv?action=getStatus',
                  headers={'Content-Length': '10',
                        'name': 'testfile.csv', 
                        'x-ms-resource-type': 'file'},
            )
    mock_abfs = test_mock_abfs
    mock_details = mock_abfs.blob_fs.info("testfile.csv")
    assert mock_details == {'name': 'testfile.csv', 'size': 10, 'type': 'file'}

@responses.activate
def test_fs_open(test_mock_abfs):
    """ Test opening an AzureBlobFile using the AzureBlobFileSystem """
    
    responses.add(responses.HEAD, 'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv?action=getStatus',
                  headers={'Content-Length': '10',
                        'name': 'testfile.csv', 
                        'x-ms-resource-type': 'file'},
            )
    responses.add(responses.GET, 'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv',
                  body=b',test\n0,0\n1,1\n2,2\n')
    mock_abfs = test_mock_abfs
    mock_file_object = mock_abfs.open('testfile.csv')
    assert mock_file_object == AzureBlobFile(fs=mock_abfs, path='testfile.csv')

#### Tests against the AzureBlobFile Class
@responses.activate
@pytest.mark.parametrize("start, end", [(None, None), (0,0), (300, 18), (3, 30)])
def test_fetch_range(test_mock_abfs, start, end):
    """ Test opening an AzureBlobFile using the AzureBlobFileSystem """
    
    responses.add(responses.HEAD, 'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv?action=getStatus',
                  headers={'Content-Length': '10',
                        'name': 'testfile.csv', 
                        'x-ms-resource-type': 'file'},
            )
    responses.add(responses.GET, 
                  'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv',
                  body=b',test\n0,0\n1,1\n2,2\n')
    mock_abfs = test_mock_abfs
    f = mock_abfs.open('testfile.csv')
    cache = f._fetch_range(start=start, end=end)
    assert cache == b',test\n0,0\n1,1\n2,2\n'

@responses.activate
@pytest.mark.parametrize('pointer_location', [0])
def test_read(test_mock_abfs, pointer_location):
    """ Test read method on AzureBlobFile """
    responses.add(responses.HEAD, 'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv?action=getStatus',
                  headers={'Content-Length': '18',
                        'name': 'testfile.csv', 
                        'x-ms-resource-type': 'file'},
            )
    responses.add(responses.GET, 
                  'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv',
                  body=b',test\n0,0\n1,1\n2,2\n')
    mock_abfs = test_mock_abfs
    mock_abf = AzureBlobFile(fs=mock_abfs, path='testfile.csv')
    mock_abf.loc = pointer_location
    out = mock_abf.read()
    assert out==b',test\n0,0\n1,1\n2,2\n'

@responses.activate
def test_fetch_range(test_mock_abfs):
    """ Test _fetch_range method, to verify that the start and end locations when cacheing files are passed properlly """
    
    # Set up the mocked API calls
    responses.add(responses.HEAD, 'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv?action=getStatus',
                  headers={'Content-Length': '18',
                        'name': 'testfile.csv', 
                        'x-ms-resource-type': 'file'},
            )
    responses.add(responses.GET,
                  'https://test_storage_account.dfs.core.windows.net/test_filesystem/testfile.csv',
                  body=b',test\n0,0\n1,1\n2,2\n')
    mock_abfs = test_mock_abfs
    mock_abf = AzureBlobFile(fs=mock_abfs, path='testfile.csv')
    