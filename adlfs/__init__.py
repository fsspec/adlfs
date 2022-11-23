from ._version import get_versions
from .spec import AzureBlobFile, AzureBlobFileSystem
from .gen1 import AzureDatalakeFileSystem
__all__ = ["AzureBlobFileSystem", "AzureBlobFile", "AzureDatalakeFileSystem"]

__version__ = get_versions()["version"]
del get_versions

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
