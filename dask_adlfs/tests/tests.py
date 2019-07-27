
import unittest
from unittest.mock import patch, PropertyMock

import pytest
from dask_adlfs.core import AzureDatalakeFileSystem

@pytest.fixture(scope='session')
def db_connection(docker_services, docker_ip):
    """
    :param docker_services: pytest-docker plugin fixture
    :param docker_ip: pytest-docker plugin fixture
    :return: psycopg2 connection class
    """
    db_settings = {
        'database'        : 'test_database',
        'user'            : 'postgres',
        'host'            : docker_ip,
        'password'        : '',
        'port'            : docker_services.port_for('database', 5432),
        'application_name': 'your-app'
    }
    dbc = psycopg2.connect(**db_settings)
    dbc.autocommit = True
    return dbc


@patch('dask_adlfs.core.AzureDLFileSystem', attribute='connect', return_value=None)
@patch.object(AzureDatalakeFileSystem, attribute='connect', return_value=None)
class TestAzureDatalakeFileSystem(unittest.TestCase):
  

  def test_trim_filename(self, mock_session, mock_conn):
    expected = mock_conn.return_value='/folder/fname'
    fs = mock_session
    output = fs._trim_filename('adl://store_name/folder/fname')
    assert output == expected

  # def test_glob(self, mock_conn):
  #   expected = mock_conn.return_value=['adl://store_name.azuredatalakestore.net/folder/fname']
  #   fs = AzureDatalakeFileSystem(tenant_id='test_tenant',
  #     client_id='test_client', client_secret='test_secret', store_name='store_name')
  #   output = fs.glob('adl://store_name/folder/fname')
  #   assert output == expected