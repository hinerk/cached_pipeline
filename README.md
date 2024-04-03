A thing(tm) to decide dynamically whether to use cached data instead of
perform the actual calculation for certain tasks in a data processing
pipeline.

creating a data pipeline:

```python
pipeline = CachedPipeline()
```

We will use a dict as the simplest cache possible:

```python
simple_cache = dict()
```

now we register a read and write method for interaction with the cache:

```python
@pipeline.write_cache
def write_cache(data, cache_id):
    simple_cache[cache_id] = data

@pipeline.read_cache
def load_cache(cache_id):
   return simple_cache[cache_id]
```

then we continue with adding tasks to the pipeline:

```python
import time
import random
from collections import defaultdict

@pipeline.task
def fetch_raw_data(size: int = 10_000):
    time.sleep(0.1)
    return [random.random() for i in range(size)]

@pipeline.task
def sanitize_raw_data(data: list[float], scale=23) -> list[float]:
    time.sleep(0.1)
    return [d * scale for d in data]

@pipeline.task
def mangle_data(data: list[float]) -> dict[int, list[float]]:
    result = defaultdict(list)
    for date in data:
        result[int(date)].append(date)

    time.sleep(0.1)
    return {k: v for k, v in result.items()}

@pipeline.task
def project_results(data: dict[int, list[float]]) -> list[dict[str, int]]:
    time.sleep(0.1)
    return [{'cookies': k, 'bananas': len(v)} for k, v in data.items()]
```

The key used to store the results of a task in the cache default to the
name of the defined function. However, it is possible to specify a
different key via the `cache_id` parameter:

```python
@pipeline.task(cache_id="make_data_accessible_to_management")
def render_excel_sheet(data: list[dict[str, int]]) -> str:
    time.sleep(0.1)
    return "\\n".join(f'{d["cookies"]};{d["bananas"]}' for d in data)
```

Finally, we may want to chain those functions into an actual pipeline:

```python
def exec_pipeline():
    begin = time.time()
    data = fetch_raw_data()
    sanitized_data = sanitize_raw_data(data)
    mangled_data = mangle_data(sanitized_data)
    projected_data = project_results(mangled_data)
    excel = render_excel_sheet(projected_data)
    duration = time.time() - begin
    logger.info(f'pipeline finished after {duration:.2f} s')
    return excel, duration
```

we expect the execution will take some time:

```python
_, duration_without_cache = exec_pipeline()
duration_without_cache >= 0.5
```

we can access the cached data of a task like:

```python
sanitize_raw_data.cached_data()
```

after activating the usage of cached data:

```python
>>> pipeline.enable_caching()
```

the pipeline should finish quicker:

```python
_, duration_cached = exec_pipeline()
duration_cached < duration_without_cache
```

disable the usage of cached data:

```python
pipeline.disable_caching()
```

The actual reason to write this data structure was to be able to skip a
bunch of tasks and head immediately to the one which requires debugging.
This can be achieved like:

```python
pipeline.skip_until_task(project_results)
_, duration_partially_cached = exec_pipeline()
duration_cached < duration_partially_cached < duration_without_cache
```
