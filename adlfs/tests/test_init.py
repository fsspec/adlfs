import pytest
import fsspec
from adlfs import AzureBlobFileSystem, AzureBlobFile


def test_adl_protocol_attribute_error():
    with pytest.raises(AttributeError, match="Gen1 has been retired since February 29, 2024 and "
            "is no longer supported by adlfs"):
        fsspec.filesystem("adl")


def test_gen1_class_attribute_error():
    import adlfs
    with pytest.raises(AttributeError, match="Gen1 has been retired since February 29, 2024 and "
            "is no longer supported by adlfs"):
        adlfs.AzureDatalakeFileSystem

def test_known_attributes():
    import adlfs
    assert adlfs.AzureBlobFileSystem is AzureBlobFileSystem
    assert adlfs.AzureBlobFile is AzureBlobFile

def test_getattr_unknown_attribute():
    import adlfs
    with pytest.raises(AttributeError, match="module 'adlfs' has no attribute 'unknown_attribute'"):
        getattr(adlfs, "unknown_attribute")