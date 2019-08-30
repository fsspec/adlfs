from unittest import mock
import pytest
import requests

from adlfs.core import AzureBlobFileSystem, AzureBlobFile

    
# @pytest.fixture(autouse=True)
# def no_requests(monkeypatch):
#     monkeypatch.delattr("requests.sessions.Session.request")

class MockResponse:
    def __init__(self):
        self.status_code = 200
        self.headers = {}
        
    def json(self):
        return {'paths': [
            {'name': 'subfolder', 'isDirectory': 'true', 'contentLength': '5'},
            {'name': 'testfile.csv', 'contentLength': '10'},
            {'name': 'testfile2.csv', 'contentLength': '20'}
            ]
        }
        
class MockResponseSingleFile:
    def __init__(self):
        self.status_code = 200
        
    def json(self):
        return {'paths': [
            {'name': 'testfile.csv', 'contentLength': '10'},
        ]}

class MockAzureBlobFile:
    def __init__(self):
        pass
    
    def content(self):
        return b'thisismyfilecontent'

@pytest.fixture(autouse=True)
def test_mock_abfs(monkeypatch):
    tenant_id = 'test_tenant'
    client_id = 'test_client'
    client_secret = 'client_secret'
    storage_account = 'test_storage_account'
    filesystem = 'test_filesystem'
    
    def mock_connect(conn):
        return "None"
    
    monkeypatch.setattr(AzureBlobFileSystem, "connect", mock_connect)
    
    mock_fs = AzureBlobFileSystem(tenant_id=tenant_id, client_id=client_id,
                                client_secret=client_secret, 
                                storage_account=storage_account, filesystem=filesystem,
                                )
    return mock_fs

def test_make_url(test_mock_abfs):
    fs = test_mock_abfs
    url = fs._make_url()
    assert url == "https://test_storage_account.dfs.core.windows.net/test_filesystem"

def test_ls(test_mock_abfs, monkeypatch):
    
    """ Test that ls returns a list of approopriate files """
    # Get the mock filesystem
    fs = test_mock_abfs
    url = fs._make_url()
    
    
    def mock_get(url, headers, params):
        return MockResponse()
    
    monkeypatch.setattr(requests, "get", mock_get)
    files = fs.ls("")
    assert files == ['subfolder', 'testfile.csv', 'testfile2.csv']
    
def test_ls_detail(test_mock_abfs, monkeypatch):
    """ Verify a directory can be found when requested """
    
    fs = test_mock_abfs
    
    def mock_get(url, headers, params):
        return MockResponse()
    monkeypatch.setattr(requests, 'get', mock_get)
    files = fs.ls(path="", detail=True)
    assert files == [
        {'name': 'subfolder', 'type': 'directory', 'isDirectory': 'true', 'size': 5},
        {'name': 'testfile.csv', 'type': 'file', 'size': 10},
        {'name': 'testfile2.csv', 'type': 'file', 'size': 20}
        ]

def test_azureblobfile_read(test_mock_abfs, monkeypatch):
    """ Test reading an AzureBlobFile """
    
    mock_abfs = test_mock_abfs
    
    def mock_get(url, headers, params):
        return MockResponseSingleFile()
    monkeypatch.setattr(requests, 'get', mock_get)
    
    abf = AzureBlobFile(fs=mock_abfs, path="testfile.csv")
    abf.read()
    
    
    