#!/usr/bin/env python

from os.path import exists

import versioneer
from setuptools import setup

setup(
    name="adlfs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Access Azure Datalake Gen1 with fsspec and dask",
    url="https://github.com/hayesgb/adlfs/",
    maintainer="Greg Hayes",
    maintainer_email="hayesgb@gmail.com",
    license="BSD",
    keywords=["file-system", "dask", "azure"],
    packages=["adlfs"],
    long_description_content_type="text/markdown",
    long_description=open("README.md").read() if exists("README.md") else "",
    install_requires=[
        "azure-datalake-store>=0.0.46,<0.1",
        "azure-storage-blob>=2.1.0,<3.0.0",
        "fsspec>=0.6.0,<1.0",
        "msrest>=0.6.11,<0.7",
        "msrestazure>=0.6.3,<0.7",
    ],
    tests_require=["pytest>5.0,<6.0", "docker"],
    zip_safe=False,
)
