import fsspec
import pytest

from adlfs import AzureBlobFile, AzureBlobFileSystem
from adlfs.tests.constants import ADL_GEN1_ERROR_MSG


def test_adl_protocol_attribute_error():
    with pytest.raises(
        AttributeError,
        match=ADL_GEN1_ERROR_MSG,
    ):
        fsspec.filesystem("adl")


def test_adl_get_filesystem_class_attribute_error():
    with pytest.raises(
        AttributeError,
        match=ADL_GEN1_ERROR_MSG,
    ):
        fsspec.get_filesystem_class("adl")


def test_adl_open_attribute_error():
    with pytest.raises(
        AttributeError,
        match=ADL_GEN1_ERROR_MSG,
    ):
        fsspec.open("adl://container/path/file.txt")


def test_gen1_class_attribute_error():
    import adlfs

    with pytest.raises(
        AttributeError,
        match=ADL_GEN1_ERROR_MSG,
    ):
        adlfs.AzureDatalakeFileSystem


def test_known_attributes():
    import adlfs

    assert adlfs.AzureBlobFileSystem is AzureBlobFileSystem
    assert adlfs.AzureBlobFile is AzureBlobFile


def test_getattr_unknown_attribute():
    import adlfs

    with pytest.raises(
        AttributeError, match="module 'adlfs' has no attribute 'unknown_attribute'"
    ):
        getattr(adlfs, "unknown_attribute")
