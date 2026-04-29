from .spec import AzureBlobFile, AzureBlobFileSystem
from .utils import __version__, version_tuple  # noqa: F401

__all__ = ["AzureBlobFileSystem", "AzureBlobFile"]


def __getattr__(name):
    msg = f"module {__name__!r} has no attribute {name!r}"
    if name == "AzureDatalakeFileSystem":
        err = (
            "Azure Data Lake Storage (ADLS) Gen1 has been retired since February 29, 2024 and "
            "is no longer supported by adlfs. It is recommended to use the az:// protocol "
            "and/or adlfs.AzureBlobFileSystem class which support Azure Blob Storage and Azure "
            "Data Lake Storage Gen2. "
        )
        msg = err + msg
    raise AttributeError(msg)
