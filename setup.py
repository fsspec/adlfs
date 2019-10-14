#!/usr/bin/env python

from os.path import exists
from setuptools import setup
from os import path


setup(name='adlfs',
      version='0.0.10post1',
      description='Access Azure Datalake Gen1 with fsspec and dask',
      url='https://github.com/hayesgb/adlfs/',
      maintainer='Greg Hayes',
      maintainer_email='hayesgb@gmail.com',
      license='BSD',
      keywords=['file-system', 'dask', 'azure'],
      packages=['adlfs'],
      long_description=open('README.rst').read() if exists('README.rst') else '',
      install_requires=['azure-datalake-store',
                        'fsspec>=0.4.0<1.0',
                        'requests>=2.11',    # After requests 2.11, ints are not allowed in headers, but the ADLSGen2 API expects position to be an int
                        'azure-storage-blob==12.0.0b4'
                        ],
      zip_safe=False)
