from .spec import AzureDatalakeFileSystem
from .spec import AzureBlobFileSystem, AzureBlobFile
from ._version import get_versions

import fsspec

__all__ = ["AzureBlobFileSystem", "AzureBlobFile", "AzureDatalakeFileSystem"]

__version__ = get_versions()["version"]
del get_versions

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

if hasattr(fsspec, "register_implementation"):
    fsspec.register_implementation("abfss", AzureBlobFileSystem, clobber=True)
else:
    from fsspec.registry import known_implementations

    known_implementations["abfss"] = {
        "class": "adlfs.AzureBlobFileSystem",
        "err": "Please install adlfs to use the abfss protocol",
    }

    del known_implementations

del fsspec  # clear the module namespace
