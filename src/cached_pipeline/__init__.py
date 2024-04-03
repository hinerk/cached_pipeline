import inspect
from typing import Callable


def _wrap_async_or_sync_function(wrapped_func, sync_wrapper, async_wrapper):
    if inspect.iscoroutinefunction(wrapped_func):
        return async_wrapper
    else:
        return sync_wrapper


def async_if_async(func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return


class CachedPipeline:
    def __init__(self):
        self._return_cached_data = False

    def use_cache(self):
        self._return_cached_data = True

    def read_cache(self, cache_id):
        ...

    def write_cache(self, data, cache_id):
        ...

    def task(self, func):
        def wrapper(*args, **kwargs):
            if self._return_cached_data:
                return self.read_cache(cache_id=func)

            refined_data = func(*args, **kwargs)

            self.write_cache(refined_data, cache_id=func)

            return refined_data

        return wrapper
