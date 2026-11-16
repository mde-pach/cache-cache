# How it works

This page explains what happens on a call, and why the main design choices were made. It also lists what the library doesn't do.

## The path of a call

For a `@cached` call:

1. **Key.** Arguments are bound to the signature, with defaults applied. `self` and ignored types are dropped, and the rest is encoded into a key. Tags are rendered from the same arguments.
2. **Session.** If the key is in the current session, return the value.
3. **In flight.** If an identical call is running in this process, wait for it.
4. **Backend.** Fetch the entry and the current versions of its tags. Every lookup made in the same event-loop iteration shares one round trip.
5. **Decide.**
    - The entry is fresh and its tag versions match: return it.
    - It's stale: serve it if a stale window allows, otherwise treat it as a miss.
    - It's missing: run the function once, in a task shared by every caller that joins. Batched functions queue the id for the next bulk call.
6. **Store.** Write the result with the tag versions read in step 4, then keep it in the caller's session.

Middleware wraps steps 5 and 6. Observers see an event at each step.

## Tag versions { #tag-versions }

Every tag has a counter in the backend. Invalidating a tag increments its counter. An entry records the counters of all its tags and their ancestors (`product`, `product:list`, `product:list:shoes`), as read **before** the function ran. On read, if any counter has moved, the entry is stale.

This design has three consequences.

**The invalidate/store race is closed.** A common sequence with delete-based invalidation:

1. Process B misses, and reads the old row from the database.
2. Process A commits a change and deletes the cache entry.
3. B writes the old row into the cache, where it stays until it expires, or forever without a ttl.

With tag versions, B's entry records the counter B saw at step 1. A increments it at step 2, so B's entry is already stale when B writes it, and it is never served as fresh.

**Hierarchical invalidation is cheap.** Invalidating `product` increments one counter, whatever the number of entries below it. There is no scan over keys.

**Reads cost a little more.** A lookup also fetches the tag counters. They're batched with the entry fetch, so this is one extra `MGET` per event-loop iteration, not per call.

## Deterministic keys

Python's `hash()` is randomized per process for strings, and identity-based for most objects, so it can't name an entry shared by several processes. Arguments are encoded into a canonical JSON structure instead:

- sets and dicts are sorted,
- types are tagged, so `1`, `1.0`, `"1"` and `True` differ,
- dataclasses and pydantic models are encoded field by field.

The key is a SHA-256 digest of that structure.

## Code fingerprints { #code-fingerprints }

Each key includes a hash of:

- the function's normalized AST, so comments, formatting, docstrings and decorators don't count,
- the structure of its return type, including the fields of dataclasses and pydantic models,
- `version=`.

Change the function's body or its return model, and old entries are no longer read.

Limits:

- **Callees aren't hashed.** If `get()` calls `_to_product()` and you change `_to_product()`, bump `version=`.
- **Python upgrades change the AST.** Upgrading the interpreter can change every fingerprint, which empties the cache once.
- **Old entries aren't deleted.** They stay until they expire or are evicted. In Redis, prefer a ttl, or an eviction policy.

## Dedupe is per process

Dedupe and batching coordinate tasks in one event loop. They don't coordinate workers. Across processes, a cold key can be computed once per process. Stale-while-revalidate helps for hot keys. Distributed locks are not implemented.

When callers share an execution:

- The execution is a separate task, and every caller waits on it through `asyncio.shield`. Cancelling the first caller doesn't cancel the others.
- Exceptions reach every waiting caller and are never stored.

## When checks run

| When | What |
|---|---|
| Import (decoration) | tag placeholders name real parameters; key, condition and result-condition signatures; bulk signature names; option values |
| `Cache.open()` | annotations resolve; argument types have a key encoding; bulk sequence annotation; the backend can store the return type |
| `cache-cache check` | everything above, plus invalidated tags match declared tags, and declared tags that nothing invalidates |

Checks that need type annotations wait until the cache opens. At decoration time, classes referenced in annotations may not be defined yet.

## Why no sync functions

The engine relies on the event loop for dedupe (shared tasks), batching (collecting calls during one iteration) and background refreshes. A sync version would need threads and locks, and would behave differently. `functools.cache` and cachetools cover sync code well.
