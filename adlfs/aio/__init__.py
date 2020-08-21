from .core import AzureBlobFileSystem, AzureBlobFile
import fsspec

__all__ = ["AzureBlobFileSystem", "AzureBlobFile"]

if hasattr(fsspec, 'register_implementation'):
    fsspec.register_implementation('abfs', AzureBlobFileSystem, clobber=True)

del fsspec

from . import caching