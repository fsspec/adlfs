from ._version import get_versions
from .spec import AzureBlobFile, AzureBlobFileSystem, AzureDatalakeFileSystem

__all__ = ["AzureBlobFileSystem", "AzureBlobFile", "AzureDatalakeFileSystem"]

__version__ = get_versions()["version"]
del get_versions

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
