
import unittest
from unittest.mock import patch
import glob

import pytest
from dask_adlfs.core import AzureDatalakeFileSystem


# class TestGlob(AzureDatalakeFileSystem):

#   def glob(self, path):
#     return glob.glob(path)


@patch.object(AzureDatalakeFileSystem, attribute='connect', return_value=None, autospec=True)
def test_trim_filename(mock_conn):
  expected = mock_conn.return_value='/folder/fname'
  fs = AzureDatalakeFileSystem()
  output = fs._trim_filename('adl://store_name/folder/fname')
  assert output == expected


@patch.object(AzureDatalakeFileSystem, attribute='connect', return_value=None, autospec=True)
def test_glob(mock_conn):
	expected = mock_conn.adl.glob().return_value='adl://store_name.azuredatalakestore.net/folder/fname'
	output = mock_conn().glob('adl://store_name/folder/fname')
	assert output == expected


