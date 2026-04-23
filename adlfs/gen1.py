# -*- coding: utf-8 -*-


class AzureDatalakeFileSystem:
    def __new__(cls, *args, **kwargs):
        raise RuntimeError(
            "Azure Data Lake Storage (ADLS) Gen1 has been retired since February 29, 2024 and "
            "is no longer supported by adlfs. It is recommended to use the az:// protocol "
            "and/or adlfs.AzureBlobFileSystem class which support Azure Blob Storage and Azure "
            "Data Lake Storage Gen2."
        )
