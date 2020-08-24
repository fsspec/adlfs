from .core import AzureDatalakeFileSystem
from .core import AzureBlobFileSystem, AzureBlobFile
from ._version import get_versions
from .aio import core as async

__all__ = ["AzureBlobFileSystem", "AzureBlobFile", "AzureDatalakeFileSystem"]

__version__ = get_versions()["version"]
del get_versions

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

# from .aio import caching