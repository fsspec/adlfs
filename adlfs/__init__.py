from ._version import get_versions
from .spec import AzureBlobFile, AzureBlobFileSystem

__all__ = ["AzureBlobFileSystem", "AzureBlobFile"]

__version__ = get_versions()["version"]
del get_versions

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
