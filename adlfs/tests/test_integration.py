import os
import random
import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import pytest
import tempfile
import yaml

from adlfs import AzureBlobFileSystem


here = Path(__file__).absolute().parent
config_file_path = here / "test-azure-blob.yml"
with config_file_path.open() as fp:
    config = yaml.safe_load(fp)

AUTH_METHODS_AND_REQUIRED_PARAMS = {
    "env_conn_str": ["AZURE_STORAGE_CONNECTION_STRING"],
    "env_sas_token": ["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_SAS_TOKEN"],
    "env_account_key": ["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT_KEY"],
    "env_spn": [
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_CLIENT_ID",
        "AZURE_STORAGE_CLIENT_SECRET",
        "AZURE_STORAGE_TENANT_ID",
    ],
    "fsspec_conn_str": ["connection_string"],
    "fsspec_sas_token": ["account_name", "sas_token"],
    "fsspec_account_key": ["account_name", "account_key"],
    "fsspec_spn": ["account_name", "client_id", "client_secret", "tenant_id"],
    "fsspec_credential": ["credential"],
}


def verify_auth_parameters_and_configure_env(auth_method):
    # This sets up the authentication method against Azure
    # if testing the use of Azure credentials stored as
    # environmental variable, it creates the environmental
    # variables and returns storage_options = None.  Otherwise
    # it returns adlfs-recognized parameters compliant with the
    # fsspec api.  These get saved as secrets by mlrun.get_dataitem()
    # for authentication.
    if not config["env"].get("AZURE_CONTAINER"):
        return False

    for k, env_vars in AUTH_METHODS_AND_REQUIRED_PARAMS.items():
        for env_var in env_vars:
            os.environ.pop(env_var, None)

    test_params = AUTH_METHODS_AND_REQUIRED_PARAMS.get(auth_method)
    if not test_params:
        return False

    if auth_method.startswith("env"):
        for env_var in test_params:
            env_value = config["env"].get(env_var)
            if not env_value:
                return False
            os.environ[env_var] = env_value

        logger.info(f"Testing auth method {auth_method}")
        return None

    elif auth_method.startswith("fsspec"):
        storage_options = {}
        for var in test_params:
            value = config["env"].get(var)
            if not value:
                return False
            storage_options[var] = value
        logger.info(f"Testing auth method {auth_method}")
        return storage_options

    else:
        raise ValueError("auth_method not known")


# Apply parametrization to all tests in this file. Skip test if auth method is not configured.
pytestmark = pytest.mark.parametrize(
    "auth_method",
    [
        pytest.param(
            auth_method,
            marks=pytest.mark.skipif(
                not verify_auth_parameters_and_configure_env(auth_method),
                reason=f"Auth method {auth_method} not configured.",
            ),
        )
        for auth_method in AUTH_METHODS_AND_REQUIRED_PARAMS
    ],
)


def test_put_azure_blob(auth_method):
    storage_options = verify_auth_parameters_and_configure_env(auth_method)
    if storage_options:
        fs = AzureBlobFileSystem(**storage_options)
    else:
        fs = AzureBlobFileSystem()

    blob_path = config["env"].get("AZURE_CONTAINER")
    blob_name= f"{blob_path}/test_file.csv"
    
    # Create a Pandas DataFrame and verify its larger than 200MB
    A = np.random.random_sample(size=(5000000, 6))
    df = pd.DataFrame(data=A, columns=list("ABCDEF"))
    assert (df.memory_usage(deep=True).sum() / 1e6) > 200
    
    with tempfile.TemporaryDirectory() as td:
        test_file: Path = Path(td) / "test_file.csv"
        df.to_csv(test_file, index=False)
        assert test_file.exists()
        now = datetime.datetime.now()
        fs.put(test_file, blob_name, overwrite=True, max_concurrency=1)
        done = datetime.datetime.now()
        diff1 = done - now
        now = datetime.datetime.now()
        fs.put(test_file, blob_name, overwrite=True)
        done = datetime.datetime.now()
        diff2 = done - now
        assert diff2 < diff1

    