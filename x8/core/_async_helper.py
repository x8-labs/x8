import asyncio
import threading
from typing import Any, Callable

_loop = None
_loop_thread = None


def _ensure_loop():
    global _loop, _loop_thread
    if _loop and not _loop.is_closed():
        return _loop

    _loop = asyncio.new_event_loop()

    def _run_loop():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    _loop_thread = threading.Thread(target=_run_loop, daemon=True)
    _loop_thread.start()
    return _loop


def run_async(func: Callable[..., Any], *args, **kwargs):
    return asyncio.to_thread(func, *args, **kwargs)


def run_sync(afunc: Callable[..., Any], *args, **kwargs):
    loop = _ensure_loop()

    coro = afunc(*args, **kwargs)
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()
