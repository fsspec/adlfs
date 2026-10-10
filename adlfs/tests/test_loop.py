import asyncio
import gc
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
from azure.identity.aio import DefaultAzureCredential

from adlfs import AzureBlobFileSystem
from adlfs.tests.constants import CONN_STR


def test_fs_loop(storage):
    """
    This is a test to verify that AzureBlobFilesystem can provide a
    running event loop to azure python sdk when requesting asynchronous
    credentials And running in a separate thread
    """

    def test_connect_async_credential():
        fs = AzureBlobFileSystem(  # NOQA
            account_name=storage.account_name, credential=DefaultAzureCredential()
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(test_connect_async_credential)
        assert future.result() is None


def test_file_loop(storage):
    """
    This is a test to verify that AzureBlobFile class provides a
    running event loop to the Azure python sdk when requesting asynchronous
    credentials and running in a separate thread
    """

    def test_connect_async_open_credential():
        fs = AzureBlobFileSystem(
            account_name=storage.account_name, credential=DefaultAzureCredential()
        )
        fs.open(path="")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(test_connect_async_open_credential)
        with pytest.raises(ValueError):
            future.result()


def test_close_shuts_down_clients():
    """
    Calling close() on a filesystem closes its service client (and credential)
    on the filesystem's own loop, and is a no-op when called again.
    """
    fs = AzureBlobFileSystem(connection_string=CONN_STR, skip_instance_cache=True)
    with mock.patch.object(fs.service_client, "close", new=mock.AsyncMock()) as m:
        fs.close()
        assert m.await_count == 1
        fs.close()
        assert m.await_count == 1


async def test_async_close_shuts_down_clients():
    """
    Calling _close() on an asynchronous filesystem (which has no loop of its
    own) closes the service client on the caller's loop.
    """
    fs = AzureBlobFileSystem(
        connection_string=CONN_STR, asynchronous=True, skip_instance_cache=True
    )
    with mock.patch.object(fs.service_client, "close", new=mock.AsyncMock()) as m:
        await fs._close()
        assert m.await_count == 1
        await fs._close()
        assert m.await_count == 1


def test_finalizer_closes_clients_on_gc():
    """
    When a filesystem is garbage collected while its loop is still running,
    the service client is closed, and the instance is actually collected (the
    finalizer must not hold a strong reference to the filesystem).
    """
    fs = AzureBlobFileSystem(connection_string=CONN_STR, skip_instance_cache=True)
    close = mock.AsyncMock()
    fs.service_client.close = close
    ref = weakref.ref(fs)
    del fs
    gc.collect()
    assert ref() is None
    assert close.await_count == 1


def test_finalizer_skipped_when_loop_closed(capfd):
    """
    When a filesystem is garbage collected after the loop it was bound to has
    been closed, the finalizer skips the cleanup instead of raising
    ``RuntimeError: Loop is not running`` (https://github.com/fsspec/adlfs/issues/495).
    """
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    fs = AzureBlobFileSystem(
        connection_string=CONN_STR, loop=loop, skip_instance_cache=True
    )
    loop.call_soon_threadsafe(loop.stop)
    thread.join()
    loop.close()

    ref = weakref.ref(fs)
    del fs
    gc.collect()
    assert ref() is None
    _, err = capfd.readouterr()
    assert "Loop is not running" not in err
    assert "Exception ignored" not in err
