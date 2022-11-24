from ._version import get_versions
from .gen1 import AzureDatalakeFileSystem
from .spec import AzureBlobFile, AzureBlobFileSystem

__all__ = ["AzureBlobFileSystem", "AzureBlobFile", "AzureDatalakeFileSystem"]

__version__ = get_versions()["version"]
del get_versions

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
