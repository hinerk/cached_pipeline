import functools
import inspect
from typing import TypeVar
from collections.abc import Coroutine
from collections.abc import Callable


_T = TypeVar('_T')


class CachedPipeline:
    def __init__(self):
        self._return_cached_data = False
        self._read_cache_func = None
        self._write_cache_func = None
        self._until_step = None

    def use_cache(self, until_step=None):
        self._return_cached_data = True
        if until_step is not None:
            if callable(until_step):
                self._until_step = until_step.__name__
            else:
                self._until_step = until_step

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

        def decorator(func: Callable[[...], _T] | Coroutine[[...], _T]):
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                if self._until_step == cache_id:
                    self._return_cached_data = False

                if self._return_cached_data:
                    return self._sync_read_cache(cache_id=cache_id)
                refined_data = func(*args, **kwargs)
                self._sync_write_cache(refined_data, cache_id)
                return refined_data

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                if self._until_step == cache_id:
                    self._return_cached_data = False

                if self._return_cached_data:
                    return await self._async_read_cache(cache_id=cache_id)
                refined_data = await func(*args, **kwargs)
                await self._async_write_cache(refined_data, cache_id)
                return refined_data

            if inspect.iscoroutinefunction(func):
                return async_wrapper

            return sync_wrapper

        # allow to decorate function with or without parameter:
        # if _func is callable, then it is assumed, _func was decorated without
        # any arguments
        if callable(_func):
            return decorator(_func)

        return decorator
