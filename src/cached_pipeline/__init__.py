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


class _Task(Protocol):
    def __call__(self, *args, **kwargs) -> _TaskReturnType: ...


_CacheIdType = _Task | str


class _ReadCache(Protocol):
    def __call__(self, cache_id: _CacheIdType) -> _TaskReturnType: ...


class _WriteCache(Protocol):
    def __call__(self, data: _TaskReturnType, cache_id: _CacheIdType) -> None:
        ...


class SourceOfReturnedData(enum.Enum):
    NOTHING = enum.auto()
    CACHED_DATA = enum.auto()
    CALCULATED_DATA = enum.auto()


class PipelineTask(Generic[_TaskReturnType]):
    def __init__(self):
        self._cache_handler = None
        """remembers the registered cache handler"""
        self._read_cache_func = None
        """remembers the registered read-from-cache method"""
        self._write_cache_func = None
        """remembers the registered write-to-cache method"""

    def _register_cache_handler(self, func: _Task) -> _Task:
        """registers the CachedPipelines cache_handler wrapping the task"""
        self._cache_handler = func
        functools.update_wrapper(self, self._cache_handler)
        return func

    def register_read_cache_method(self, func: _ReadCache) -> _ReadCache:
        """registers a custom read-from-cache method for this Task"""
        self._read_cache_func = func
        return func

    def register_write_cache_method(self, func: _WriteCache) -> _WriteCache:
        """registers a custom write-to-cache method for this Task"""
        self._write_cache_func = func
        return func

    def __call__(self, *args, **kwargs) -> _TaskReturnType:
        return self._cache_handler(*args, **kwargs)

    def cached_data(self) -> _TaskReturnType:
        return self._read_cache_func()


class AsyncPipelineTask(PipelineTask):
    async def __call__(self, *args, **kwargs) -> _TaskReturnType:
        return await self._cache_handler(*args, **kwargs)

    async def cached_data(self) -> _TaskReturnType:
        return await self._read_cache_func()


class CachedPipeline:
    """
    A thing(tm) to decide dynamically whether to use cached data instead of
    perform the actual calculation for certain tasks in a data processing
    pipeline.

    Example
    -------

    creating a data pipeline:
    >>> pipeline = CachedPipeline()

    We will use a dict as the simplest cache possible:
    >>> simple_cache = dict()

    now we register a read and write method for interaction with the cache:
    >>> @pipeline.write_cache
    ... def write_cache(data, cache_id):
    ...     simple_cache[cache_id] = data
    ...
    >>> @pipeline.read_cache
    ... def load_cache(cache_id):
    ...    return simple_cache[cache_id]

    then we continue with adding tasks to the pipeline:

    >>> import time
    >>> import random
    >>> from collections import defaultdict
    ...
    >>> @pipeline.task
    ... def fetch_raw_data(size: int = 10_000):
    ...     time.sleep(0.1)
    ...     return [random.random() for i in range(size)]
    ...
    >>> @pipeline.task
    ... def sanitize_raw_data(data: list[float], scale=23) -> list[float]:
    ...     time.sleep(0.1)
    ...     return [d * scale for d in data]
    ...
    >>> @pipeline.task
    ... def mangle_data(data: list[float]) -> dict[int, list[float]]:
    ...     result = defaultdict(list)
    ...     for date in data:
    ...         result[int(date)].append(date)
    ...
    ...     time.sleep(0.1)
    ...     return {k: v for k, v in result.items()}
    ...
    >>> @pipeline.task
    ... def project_results(data: dict[int, list[float]]) -> list[dict[str, int]]:
    ...     time.sleep(0.1)
    ...     return [{'cookies': k, 'bananas': len(v)} for k, v in data.items()]

    The key used to store the results of a task in the cache default to the
    name of the defined function. However, it is possible to specify a
    different key via the `cache_id` parameter:
    >>> @pipeline.task(cache_id="make_data_accessible_to_management")
    ... def render_excel_sheet(data: list[dict[str, int]]) -> str:
    ...     time.sleep(0.1)
    ...     return "\\n".join(f'{d["cookies"]};{d["bananas"]}' for d in data)
    ...

    Finally, we may want to chain those functions into an actual pipeline:
    >>> def exec_pipeline():
    ...     begin = time.time()
    ...     data = fetch_raw_data()
    ...     sanitized_data = sanitize_raw_data(data)
    ...     mangled_data = mangle_data(sanitized_data)
    ...     projected_data = project_results(mangled_data)
    ...     excel = render_excel_sheet(projected_data)
    ...     duration = time.time() - begin
    ...     logger.info(f'pipeline finished after {duration:.2f} s')
    ...     return excel, duration

    we expect the execution will take some time:
    >>> _, duration_without_cache = exec_pipeline()
    >>> duration_without_cache >= 0.5
    True

    we can access the cached data of a task like:
    >>> sanitize_raw_data.cached_data()  # doctest: +ELLIPSIS
    [...]

    after activating the usage of cached data:
    >>> pipeline.enable_caching()

    the pipeline should finish quicker:
    >>> _, duration_cached = exec_pipeline()
    >>> duration_cached < duration_without_cache
    True

    disable the usage of cached data:
    >>> pipeline.disable_caching()

    The actual reason to write this data structure was to be able to skip a
    bunch of tasks and head immediately to the one which requires debugging.
    This can be achieved like:
    >>> pipeline.skip_until_task(project_results)
    >>> _, duration_partially_cached = exec_pipeline()
    >>> duration_cached < duration_partially_cached < duration_without_cache
    True
    """
    def __init__(self):
        self._return_cached_data = False
        """remembers whether to return cached data"""
        self._read_cache_func: _ReadCache | None = None
        """remembers the registered default read-from-cache method"""
        self._write_cache_func: _WriteCache | None = None
        """remembers the registered default write-cache method"""
        self._skip_until_task: str | None = None
        """remembers up to which task the pipeline is skipped"""
        self._cache_aliases: dict[_Task, str] = dict()
        """maps task_id to actual task function"""

    def skip_until_task(self, task: _CacheIdType):
        """
        skip tasks in pipeline up to given task. Cached data is returned for
        the given task. All subsequent steps in the pipeline will run normally.

        :param task: the task for which cached data is used
        :return: nothing
        """
        if isinstance(task, str):
            self._skip_until_task = task
        else:
            self._skip_until_task = self._cache_aliases[task]
        logger.debug("skipping execution until %s", self._skip_until_task)

    def enable_caching(self):
        """enable the use of cached data"""
        self._return_cached_data = True

    def disable_caching(self):
        """disable the use of cached data"""
        self._return_cached_data = False
        self._skip_until_task = None

    def read_cache(self, func: _ReadCache):
        """decorates generic read-from-cache function"""
        self._read_cache_func = func
        return func

    def write_cache(self, func: _WriteCache):
        """decorates generic write-to-cache function"""
        self._write_cache_func = func
        return func

    def task(
            self,
            wrapped_task: _Task | None = None,
            cache_id: _CacheIdType | None = None
    ) -> PipelineTask | Callable[[_Task], PipelineTask]:
        """
        decorates a task of the pipeline

        can be used with or without arguments, like:
        
        >>> pipeline = CachedPipeline()
        >>> @pipeline.task
        ... def my_task(): ...
        >>> @pipeline.task(cache_id="a-different-cache-id")
        ... def my_other_task(): ...
        
        where for `my_task` the cache ID defaults to the function name, while
        for `my_other_task` the cache ID is altered to "a-different-cache-id".
        
        :param wrapped_task: the function which shall be part of the pipeline
        :param cache_id: the ID which is used to store and find data for
        this function in the cache.
        :return: wrapped_task
        """
        if cache_id is None:
            cache_id = wrapped_task.__name__

        def decorator(func: _Task):
            """
            `wrapped_task` is wrapped either in async_cache_handler or
            synchronous_cache_handler. Both *cache_handler rely on the same
            logic (`determine_data_source()`) which decides whether to
            return cached data or to run `wrapped_task`. Asynchronous and
            synchronous cache handler differ in such that the asynchronous one
            is a coroutine and as such awaits reading and writing to cache
            as well as awaiting `wrapped_task`.
            Further, `cache_handler(wrapped_function)` is then wrapped in
            either PipelineTask or AsyncPipelineTask. The purpose of
            PipelineTask is to allow for the customization of the caching
            methods of each step, like:

            >>> pipeline = CachedPipeline()
            >>> @pipeline.task
            ... def my_task(): ...
            >>> my_task.register_read_cache_method
            ... def a_custom_read_cache_method(): ...

            Finally, the registered default methods for reading and writing
            the cache are asigned to the new PipelineTask.
            """

            if asyncio.iscoroutinefunction(func):
                task = AsyncPipelineTask()
            else:
                task = PipelineTask()

            def determine_data_source():
                """
                determines whether to actually perform the step to calculate
                data or return cached data or return nothing at all.
                """
                if self._skip_until_task is not None:
                    if self._skip_until_task == cache_id:
                        self._skip_until_task = None
                        return SourceOfReturnedData.CACHED_DATA
                    return SourceOfReturnedData.NOTHING
                if self._return_cached_data:
                    return SourceOfReturnedData.CACHED_DATA
                return SourceOfReturnedData.CALCULATED_DATA

            if asyncio.iscoroutinefunction(func):   # * TASK IS A COROUTINE ***
                # noinspection PyProtectedMember
                @task._register_cache_handler
                @functools.wraps(func)
                async def async_cache_handler(
                        *args, **kwargs) -> _TaskReturnType:
                    data_source = determine_data_source()

                    if data_source == SourceOfReturnedData.CACHED_DATA:
                        return await task.cached_data()
                    elif data_source == SourceOfReturnedData.NOTHING:
                        return None
                    elif data_source == SourceOfReturnedData.CALCULATED_DATA:
                        data = await func(*args, **kwargs)
                        await task._write_cache_func(data=data)
                        return data

                    raise RuntimeError(
                        f'not clear how to handle {data_source}')

                @task.register_read_cache_method
                async def async_default_read_cache() -> _TaskReturnType:
                    if inspect.iscoroutinefunction(self._read_cache_func):
                        return await self._read_cache_func(cache_id=cache_id)
                    return self._read_cache_func(cache_id=cache_id)

                @task.register_write_cache_method
                async def async_default_write_cache(data: _TaskReturnType):
                    if inspect.iscoroutinefunction(self._write_cache_func):
                        await self._write_cache_func(
                            data=data, cache_id=cache_id)
                    return await self._write_cache_func(
                        data=data, cache_id=cache_id)
            else:   # # ****************************** TASK IS NO COROUTINE ***
                # noinspection PyProtectedMember
                @task._register_cache_handler
                @functools.wraps(func)
                def synchronous_cache_handler(
                        *args, **kwargs) -> _TaskReturnType:
                    data_source = determine_data_source()

                    if data_source == SourceOfReturnedData.CACHED_DATA:
                        return task.cached_data()
                    elif data_source == SourceOfReturnedData.NOTHING:
                        return None
                    elif data_source == SourceOfReturnedData.CALCULATED_DATA:
                        data = func(*args, **kwargs)
                        task._write_cache_func(data=data)
                        return data

                    raise RuntimeError(
                        f'not clear how to handle {data_source}')

                @task.register_read_cache_method
                def synchronous_default_read_cache() -> _TaskReturnType:
                    return self._read_cache_func(cache_id=cache_id)

                @task.register_write_cache_method
                def synchronous_default_write_cache(data: _TaskReturnType):
                    self._write_cache_func(data=data, cache_id=cache_id)

            self._cache_aliases[task] = cache_id
            return task

        # allow to decorate function with or without parameter:
        # if _func is callable, then it is assumed, _func was decorated without
        # any arguments
        if callable(wrapped_task):
            return decorator(wrapped_task)

        return decorator
