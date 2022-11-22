#!/usr/bin/env python

from os.path import exists

from setuptools import setup

import versioneer

setup(
    name="adlfs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Access Azure Datalake Gen1 with fsspec and dask",
    url="https://github.com/dask/adlfs/",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    maintainer="Greg Hayes",
    maintainer_email="hayesgb@gmail.com",
    license="BSD",
    keywords=["file-system", "dask", "azure"],
    packages=["adlfs"],
    python_requires=">=3.8",
    long_description_content_type="text/markdown",
    long_description=open("README.md").read() if exists("README.md") else "",
    install_requires=[
        "azure-core>=1.23.1,<2.0.0",
        "azure-identity",
        "azure-storage-blob>=12.12.0",
        "fsspec>=2021.10.1",
        "aiohttp>=3.7.0",
    ],
    extras_require={
        "docs": ["sphinx", "myst-parser", "furo", "numpydoc"],
        "AzureDatalakeFileSystem": ["azure-datalake-store>=0.0.46,<0.1"],
    },
    tests_require=["pytest", "docker", "pytest-mock", "arrow", "dask[dataframe]"],
    zip_safe=False,
    entry_points={
        "fsspec.specs": [
            "abfss=adlfs.AzureBlobFileSystem",
        ],
    },
)
