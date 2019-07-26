

import pytest
from pytest_mock import mocker
import dask_adlfs




@pytest.fixture
def mock_client(mocker):
    mocker.patch('dask_adlfs.core.AzureDatalakeFileSystem')
    adl_client = dask_adlfs.core.AzureDatalakeFileSystem()
    adl_client.adl = mocker.Mock()
    adl_client._trim_filename.return_value='/folder/fname'
    adl_client.glob.return_value='adl://test_store.azuredatalakestore.net/folder/filename.ftype'
    return adl_client

def test_trim_filename(mock_client):
    assert mock_client._trim_filename('adl://store_name/folder/fname') == '/folder/fname'


def test_glob(mock_client):
    """ Test passing a single file to glob"""
    assert mock_client.glob('adl://test_store/folder/filename.ftype') == \
         'adl://test_store.azuredatalakestore.net/folder/filename.ftype'