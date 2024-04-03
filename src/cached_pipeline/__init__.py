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
        cache_id = func.__name__

        def wrapper(*args, **kwargs):
            if self._return_cached_data:
                return self.read_cache(cache_id=cache_id)

            refined_data = func(*args, **kwargs)

            self.write_cache(refined_data, cache_id=cache_id)

            return refined_data

        return wrapper
