# Stale values

Two options serve an old value instead of waiting, or instead of failing. Both need a `ttl`.

```python
@cached(ttl=60, tags=["fx"], stale_while_revalidate=300, stale_if_error=3600)
async def rates(self) -> Rates: ...
```

## Stale-while-revalidate

For `stale_while_revalidate` seconds after an entry stops being fresh, either because it expired or because one of its tags was invalidated:

- the old value is returned immediately,
- one background refresh runs, even when many callers see the stale value,
- the refresh stores the new value for the next callers.

The refresh runs outside any session or transaction, so it never writes into a request that has finished. When the cache closes, it waits up to `shutdown_timeout` seconds for refreshes, then cancels them.

!!! note "Read your own writes"
    A stale value is never served for a tag invalidated in your own session. After `update()` invalidates `product:42`, the same request gets the new product, not the stale one.

## Stale-if-error

For `stale_if_error` seconds after an entry stops being fresh, if the function raises, the old value is returned instead of the exception. The exception is still reported to observers, as an `error` event followed by a `stale_if_error` event.

## How long entries stay in the backend

An entry is kept for `ttl + max(stale_while_revalidate, stale_if_error)` seconds. The stale window is measured from the end of freshness, `stored_at + ttl`, for invalidated entries too.
