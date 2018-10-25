#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='dask-adlfs',
      version='0.0.1',
      description='Access Azure Datalake Storage from Dask',
      url='https://github.com/dask/dask-adlfs/',
      maintainer='Martin Durant',
      maintainer_email='martin.durant@utoronto.ca',
      license='BSD',
      keywords=['file-system', 'dask', 'azure'],
      packages=['dask_adlfs'],
      long_description=open('README.rst').read() if exists('README.rst') else '',
      install_requires=['azure-datalake-store', 'dask'],
      zip_safe=False)
