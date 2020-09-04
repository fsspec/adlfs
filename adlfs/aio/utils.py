import asyncio
import functools
import inspect
import re
import sys
import threading

from fsspec.utils import other_paths
from fsspec.spec import AbstractFileSystem

# this global variable holds whether this thread is running async or not
thread_state = threading.local()
private = re.compile("_[^_]")

def sync_generator(loop, func, *args, callback_timeout=None, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """

    e = threading.Event()
    main_tid = threading.get_ident()
    result = [None]
    error = [False]

    async def f():
        try:
            if main_tid == threading.get_ident():
                raise RuntimeError("sync() called from thread of running loop")
            await asyncio.sleep(0)
            thread_state.asynchronous = True
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = asyncio.wait_for(future, callback_timeout)
            try:
                result[0] = await future
            except TypeError:
                result[0] = [f async for f in future]
        except Exception:
            error[0] = sys.exc_info()
        finally:
            thread_state.asynchronous = False
            e.set()

    asyncio.run_coroutine_threadsafe(f(), loop=loop)
    if callback_timeout is not None:
        if not e.wait(callback_timeout):
            raise TimeoutError("timed out after %s s." % (callback_timeout,))
    else:
        while not e.is_set():
            e.wait(10)
    if error[0]:
        typ, exc, tb = error[0]
        raise exc.with_traceback(tb)
    else:
        return result[0]
