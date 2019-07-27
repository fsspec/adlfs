# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import base64
import os
import time
from azure.datalake.store import core, lib, multithread
from azure.datalake.store.lib import auth, DataLakeCredential
from tests import fake_settings
PRINCIPAL_TOKEN = lib.auth(tenant_id=os.environ['azure_tenant_id'], client_secret=os.environ['azure_service_principal_secret'], client_id=os.environ['azure_service_principal'])
TOKEN = PRINCIPAL_TOKEN
STORE_NAME = os.environ['azure_data_lake_store_name']
TENANT_ID = fake_settings.TENANT_ID
SUBSCRIPTION_ID = fake_settings.SUBSCRIPTION_ID
RESOURCE_GROUP_NAME = fake_settings.RESOURCE_GROUP_NAME
RECORD_MODE = os.environ.get('RECORD_MODE', 'all').lower()
AZURE_ACL_TEST_APPID = os.environ.get('AZURE_ACL_TEST_APPID')
CLIENT_ID = os.environ['azure_service_principal']
'''
RECORD_MODE = os.environ.get('RECORD_MODE', 'none').lower()

if RECORD_MODE == 'none':
    STORE_NAME = fake_settings.STORE_NAME
    TENANT_ID = fake_settings.TENANT_ID
    TOKEN = DataLakeCredential(
        dict(
            access=str(base64.b64encode(os.urandom(1420))),
            refresh=str(base64.b64encode(os.urandom(718))),
            time=time.time(), client='common',
            resource="https://datalake.azure.net/",
            tenant=TENANT_ID, expiresIn=3600,
            tokenType='Bearer'))
    SUBSCRIPTION_ID = fake_settings.SUBSCRIPTION_ID
    RESOURCE_GROUP_NAME = fake_settings.RESOURCE_GROUP_NAME
    PRINCIPAL_TOKEN = DataLakeCredential(
        dict(
            access=str(base64.b64encode(os.urandom(1420))),
            client='e6f90497-409b-4a4e-81f2-8cede2fe6a65', # arbitrary guid.
            secret=str(base64.b64encode(os.urandom(1420))),
            refresh=None,
            time=time.time(),
            resource="https://datalake.azure.net/",
            tenant=TENANT_ID, expiresIn=3600,
            tokenType='Bearer'))
else:
    STORE_NAME = os.environ['azure_data_lake_store_name']
    TENANT_ID = os.environ.get('azure_tenant_id', 'common')
    TOKEN = auth(TENANT_ID,
                 os.environ['azure_username'],
                 os.environ['azure_password'])
    # set the environment variable to empty to avoid confusion in auth
    to_reset_user = os.environ.pop('azure_username', None)
    to_reset_pass = os.environ.pop('azure_password', None)
    
    PRINCIPAL_TOKEN = auth(TENANT_ID,
                           client_id=os.environ['azure_service_principal'],
                           client_secret=os.environ['azure_service_principal_secret'])

    # set it back after auth.
    if to_reset_pass:
        os.environ['azure_password'] = to_reset_pass
    if to_reset_user:
        os.environ['azure_username'] = to_reset_user

    SUBSCRIPTION_ID = os.environ['azure_subscription_id']
    RESOURCE_GROUP_NAME = os.environ['azure_resource_group_name']
'''