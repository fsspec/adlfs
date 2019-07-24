import pytest
from dask_adlfs.core import AzureDatalakeFileSystem

def test_trim_filename():
    ADLProtocol = AzureDatalakeFileSystem(token='dummy')
    so = ADLProtocol._trim_filename("adl://store_name/folder/fname")
    assert so == '/folder/fname'

def test_glob():
    """ Test passing a single file to glob"""
    ADLProtocol = AzureDatalakeFileSystem(token='dummy', store_name='test_store')
    pathlist = ADLProtocol.glob('adl://test_store/folder/filename.parq')
    assert pathlist is None
    # assert pathlist[0] == 'adl//test_store.azuredatalakestore.net/folder/filename.part'