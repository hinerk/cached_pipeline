import asyncio
import functools
import inspect
import enum
from typing import TypeVar
from typing import Protocol
from typing import Generic
from typing import Callable
from logging import getLogger


logger = getLogger(__name__)


_TaskReturnType = TypeVar('_TaskReturnType')
_CacheIdType = TypeVar('_CacheIdType')


class _Function(Protocol):
    def __call__(self, *args, **kwargs) -> _TaskReturnType: ...


class _ReadCacheFunction(Protocol):
    def __call__(self) -> _TaskReturnType: ...


class _WriteCacheFunction(Protocol):
    def __call__(self,  data: _TaskReturnType) -> None: ...


class WhatToReturn(enum.Enum):
    NOTHING = enum.auto()
    CACHED_DATA = enum.auto()
    CALCULATED_DATA = enum.auto()


class PipelineTask(Generic[_TaskReturnType]):
    def __init__(self):
        self._func = None
        self._read_cache = None
        self._write_cache = None
        self._cache_id = None

    def _function(self, func):
        self._func = func
        functools.update_wrapper(self, self._func)
        return func

    def read_cache(self, func: _ReadCacheFunction) -> _ReadCacheFunction:
        """decorates a custom read cache function"""
        self._read_cache = func
        return func

    def write_cache(self, func: _WriteCacheFunction) -> _WriteCacheFunction:
        """decorates a custom write cache function"""
        self._write_cache = func
        return func

    def __call__(self, *args, **kwargs) -> _TaskReturnType:
        data = self._func(*args, **kwargs)
        self._write_cache(data=data)
        return data

    def cached_data(self) -> _TaskReturnType:
        return self._read_cache()


class AsyncPipelineTask(PipelineTask):
    async def __call__(self, *args, **kwargs) -> _TaskReturnType:
        data = await self._func(*args, **kwargs)
        await self._write_cache(data=data)
        return data

    async def cached_data(self) -> _TaskReturnType:
        return await self._read_cache()


class CachedPipeline:
    def __init__(self):
        self._return_cached_data = False
        self._read_cache_func = None
        self._write_cache_func = None
        self._skip_until_task = None
        self._cache_aliases = dict()

    def skip_until_task(self, task):
        self._skip_until_task = self._cache_aliases[task]
        logger.debug("skipping execution until %s", self._skip_until_task)

    def enable_caching(self):
        """enable the use of cached data"""
        self._return_cached_data = True

    def disable_caching(self):
        """disable the use of cached data"""
        self._return_cached_data = False
        self._skip_until_task = None

    def read_cache(self, func):
        """decorates generic read-from-cache function"""
        self._read_cache_func = func
        return func

    def write_cache(self, func):
        """decorates generic write-to-cache function"""
        self._write_cache_func = func
        return func

    def task(self, _func=None, cache_id=None) -> PipelineTask | Callable[[_Function], PipelineTask]:
        """cache response of decorated function"""
        if cache_id is None:
            cache_id = _func.__name__

        def decorator(func: _Function):
            if asyncio.iscoroutinefunction(func):
                task = AsyncPipelineTask()
            else:
                task = PipelineTask()

            def what_to_return_logic():
                if self._skip_until_task is not None:
                    if self._skip_until_task == cache_id:
                        self._skip_until_task = None
                        return WhatToReturn.CACHED_DATA
                    return WhatToReturn.NOTHING
                if self._return_cached_data:
                    return WhatToReturn.CACHED_DATA
                return WhatToReturn.CALCULATED_DATA

            if asyncio.iscoroutinefunction(func):
                @task._function
                @functools.wraps(func)
                async def async_wrapper(*args, **kwargs):
                    what_to_return = what_to_return_logic()

                    if what_to_return == WhatToReturn.CACHED_DATA:
                        return await task.cached_data()
                    elif what_to_return == WhatToReturn.NOTHING:
                        return None
                    elif what_to_return == WhatToReturn.CALCULATED_DATA:
                        return await func(*args, **kwargs)

                    raise RuntimeError(
                        f'not clear how to handle {what_to_return}')

                @task.read_cache
                async def read_cache() -> _TaskReturnType:
                    if inspect.iscoroutinefunction(self._read_cache_func):
                        return await self._read_cache_func(cache_id=cache_id)
                    return self._read_cache_func(cache_id=cache_id)

                @task.write_cache
                async def write_cache(data: _TaskReturnType):
                    if inspect.iscoroutinefunction(self._write_cache_func):
                        await self._write_cache_func(data=data, cache_id=cache_id)
                    return await self._write_cache_func(data=data, cache_id=cache_id)
            else:
                @task._function
                @functools.wraps(func)
                def sync_wrapper(*args, **kwargs):
                    what_to_return = what_to_return_logic()

                    if what_to_return == WhatToReturn.CACHED_DATA:
                        return task.cached_data()
                    elif what_to_return == WhatToReturn.NOTHING:
                        return None
                    elif what_to_return == WhatToReturn.CALCULATED_DATA:
                        return func(*args, **kwargs)

                    raise RuntimeError(
                        f'not clear how to handle {what_to_return}')

                @task.read_cache
                def read_cache() -> _TaskReturnType:
                    return self._read_cache_func(cache_id=cache_id)

                @task.write_cache
                def write_cache(data: _TaskReturnType):
                    self._write_cache_func(data=data, cache_id=cache_id)

            self._cache_aliases[task] = cache_id
            return task

        # allow to decorate function with or without parameter:
        # if _func is callable, then it is assumed, _func was decorated without
        # any arguments
        if callable(_func):
            return decorator(_func)

        return decorator
