# Lifetimes

A result can live in three places:

| Layer | Present when | Holds a result |
|---|---|---|
| In flight | always | while the call runs: identical concurrent calls join it |
| Session | inside `async with cache.session()` | until the session closes; checked first |
| Backend | while the cache is open | until `ttl` expires or a tag is invalidated |

A `@cached` call checks the session, then the calls in flight, then the backend. On a miss it runs the function once, stores the result in the backend, and keeps it in the caller's session.

## `@cached` and `ttl`

```python
@cached                       # the cache's default ttl (Cache(ttl=...), 3600 unless set)
async def a() -> int: ...

@cached(ttl=600)              # fresh for 10 minutes
async def b() -> int: ...

@cached(ttl=None)             # no expiry: only tags or explicit invalidation remove it
async def c() -> int: ...
```

`ttl` must be positive. An entry without expiry is safe with tags because of [tag versioning](../design.md#tag-versions). It still occupies backend memory, so for Redis, set a `maxmemory` eviction policy.

## Session only

```python
@cached(session_only=True)
async def current_user_permissions(self, user_id: int) -> set[str]: ...
```

Results are kept until the session closes and never reach the backend. This is the per-request cache that DataLoader-style code needs. Without an open session, calls are only deduped.

## `@dedupe`

```python
@dedupe
async def rates(self) -> Rates: ...
```

Nothing is stored. Concurrent identical calls share one execution:

- If the caller that started the execution is cancelled, the callers that joined still get the result.
- Exceptions reach every caller and are never stored.
- Dedupe works without an open cache, and per process: it doesn't coordinate workers.

## Sessions

```python
async with cache.session(max_entries=10_000) as session:
    ...
```

- `max_entries` bounds the session (least recently used entries go first).
- A session is a snapshot. Invalidations made in this process drop matching session entries. Invalidations made by other processes don't, so a request sees consistent data from start to end.
- Sessions hand out the same object to every caller, and so does `MemoryBackend`. Treat cached values as read-only, or use frozen dataclasses and models.

## Outside a session

| Situation | Behaviour |
|---|---|
| No open cache | the function runs. `@dedupe` and batching still apply |
| Cache open, no session | backend and dedupe, no session layer |
| `configure(strict=True)`, no session | `NoActiveSession` is raised |
| `with cache_cache.disabled():` | nothing is cached or deduped; batched functions still batch |
