FROM continuumio/miniconda3

RUN conda install -q -y -c conda-forge python=3 pre-commit azure-datalake-store azure-storage-blob fsspec pytest requests
COPY . /adlfs
WORKDIR /adlfs
