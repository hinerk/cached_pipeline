import functools
import inspect
import enum
from typing import TypeVar
from collections.abc import Coroutine
from collections.abc import Callable


_T = TypeVar('_T')


class WhatToReturn(enum.Enum):
    NOTHING = enum.auto()
    CACHED_DATA = enum.auto()
    CALCULATED_DATA = enum.auto()


class CachedPipeline:
    def __init__(self):
        self._return_cached_data = False
        self._read_cache_func = None
        self._write_cache_func = None
        self._use_cache_until_task = None
        self._skip_until_task = None

    def skip_until_task(self, task):
        if callable(task):
            self._skip_until_task = task.__name__
        else:
            self._skip_until_task = task

    def enable_caching(self):
        """enable the use of cached data"""
        self._return_cached_data = True

    def disable_caching(self):
        """disable the use of cached data"""
        self._return_cached_data = False
        self._skip_until_task = None

    def read_cache(self, func):
        self._read_cache_func = func
        return func

    def write_cache(self, func):
        self._write_cache_func = func
        return func

    def _sync_read_cache(self, cache_id):
        if inspect.iscoroutinefunction(self._read_cache_func):
            raise RuntimeError("Na!")
        return self._read_cache_func(cache_id=cache_id)

    def _sync_write_cache(self, data, cache_id):
        if inspect.iscoroutinefunction(self._write_cache_func):
            raise RuntimeError("Na!")
        return self._write_cache_func(data=data, cache_id=cache_id)

    async def _async_read_cache(self, cache_id):
        if inspect.iscoroutinefunction(self._read_cache_func):
            return await self._read_cache_func(cache_id=cache_id)
        return self._read_cache_func(cache_id=cache_id)

    async def _async_write_cache(self, data, cache_id):
        if inspect.iscoroutinefunction(self._write_cache_func):
            return await self._write_cache_func(data=data, cache_id=cache_id)
        return self._write_cache_func(data=data, cache_id=cache_id)

    def task(self, _func=None, cache_id=None):
        """cache response of decorated function"""
        if cache_id is None:
            cache_id = _func.__name__

        def what_to_return_logic():
            if self._skip_until_task is not None:
                if self._skip_until_task == cache_id:
                    self._skip_until_task = None
                    return WhatToReturn.CACHED_DATA
                return WhatToReturn.NOTHING
            if self._return_cached_data:
                return WhatToReturn.CACHED_DATA
            return WhatToReturn.CALCULATED_DATA

        def decorator(func: Callable[[...], _T] | Coroutine[[...], _T]):
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                what_to_return = what_to_return_logic()

                if what_to_return == WhatToReturn.CACHED_DATA:
                    return self._sync_read_cache(cache_id=cache_id)
                elif what_to_return == WhatToReturn.NOTHING:
                    return None
                elif what_to_return == WhatToReturn.CALCULATED_DATA:
                    refined_data = func(*args, **kwargs)
                    self._sync_write_cache(refined_data, cache_id)
                    return refined_data

                raise RuntimeError(f'not clear how to handle {what_to_return}')

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                what_to_return = what_to_return_logic()

                if what_to_return == WhatToReturn.CACHED_DATA:
                    return await self._async_read_cache(cache_id=cache_id)
                elif what_to_return == WhatToReturn.NOTHING:
                    return None
                elif what_to_return == WhatToReturn.CALCULATED_DATA:
                    refined_data = await func(*args, **kwargs)
                    await self._async_write_cache(refined_data, cache_id)
                    return refined_data

                raise RuntimeError(f'not clear how to handle {what_to_return}')

            if inspect.iscoroutinefunction(func):
                return async_wrapper

            return sync_wrapper

        # allow to decorate function with or without parameter:
        # if _func is callable, then it is assumed, _func was decorated without
        # any arguments
        if callable(_func):
            return decorator(_func)

        return decorator
