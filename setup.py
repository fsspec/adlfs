#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='adlfs',
      version='0.0.2',
      description='Access Azure Datalake Gen1 with fsspec and dask',
      url='https://github.com/hayesgb/adlfs/',
      maintainer='Greg Hayes',
      maintainer_email='hayesgb@gmail.com',
      license='BSD',
      keywords=['file-system', 'dask', 'azure'],
      packages=['adlfs'],
      long_description=open('README.rst').read() if exists('README.rst') else '',
      install_requires=['azure-datalake-store', 'dask', 'fsspec'],
      zip_safe=False)
