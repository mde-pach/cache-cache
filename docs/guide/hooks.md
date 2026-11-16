# Hooks

Every cached call carries a `Lookup`: what the cache knows before the function runs. Observers read events, and middleware can change behaviour.

```python
@dataclass(frozen=True)
class Lookup:
    state: Literal["miss", "hit", "stale", "in_flight"]
    layer: Literal["session", "backend"] | None   # where the entry was found
    age: float | None                             # seconds since stored, or since the call started
```

## Observers

An observer is any object with `on_event(event: CacheEvent) -> None`. It must be fast. Exceptions it raises are logged and ignored.

```python
from cache_cache import Cache, CacheEvent


class SlowCalls:
    def on_event(self, event: CacheEvent) -> None:
        if event.kind == "call" and event.duration and event.duration > 1:
            print(f"{event.name} took {event.duration:.1f}s")


cache = Cache(observers=[SlowCalls()])
```

| `event.kind` | Meaning |
|---|---|
| `hit` | a fresh value was served (`layer` says from where) |
| `miss` | nothing usable: the function will run |
| `stale` | a stale value was served (stale-while-revalidate) |
| `stale_if_error` | the function raised; a stale value was served instead |
| `joined` | the caller waited for an identical call in flight |
| `call` | the function, or the bulk function, actually ran (`duration`) |
| `batch` | a bulk call was sent (`size` ids) |
| `store` | a result was written to the backend |
| `bypass` | caching skipped: condition false, or a pending transaction tag |
| `invalidate` | tags were invalidated (`tags`) |
| `error` | the function or the backend raised (`error`) |

`event.function` is the decorated function. For bulk calls it is the single-item function they serve. `event.name` is its dotted name.

## Built-in observers

```python
from cache_cache import LoggingObserver, StatsObserver

stats = StatsObserver()
cache = Cache(observers=[stats, LoggingObserver()])

...
s = stats.stats["myapp.repo.ProductRepo.get"]
s.hits, s.session_hits, s.misses, s.stale, s.joined, s.calls, s.batch_sizes, s.hit_ratio
```

`LoggingObserver` logs to the `cache_cache` logger: every event at `DEBUG`, errors at `WARNING`. You can pass your own `logger=` and `level=`.

## Middleware

Middleware wraps every cached call, outermost first. It receives a `CachedCall` (`function`, `instance`, `args`, `kwargs`, `key`, `lookup`) and the next handler:

- `call_next(call)` does what the cache would do: serve the hit, join the call in flight, or run.
- `call.run()` starts a fresh execution, ignoring the cache and the calls in flight. The result is still stored.
- Returning a value directly short-circuits the call.

```python
class DontJoinOldCalls:
    def __init__(self, max_age: float = 2.0) -> None:
        self.max_age = max_age

    async def __call__(self, call: CachedCall, call_next: Next) -> Any:
        if call.lookup.state == "in_flight" and call.lookup.age > self.max_age:
            return await call.run()          # start a fresh call instead of waiting
        return await call_next(call)


cache = Cache(middleware=[DontJoinOldCalls(max_age=2)])
```

`DontJoinOldCalls` is built in.

!!! note
    Middleware runs for calls that reach the cache. Calls bypassed by a condition, a pending transaction tag or `disabled()` don't go through it.
