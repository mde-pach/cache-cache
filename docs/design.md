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

## Deployments instead of code fingerprints { #deployments }

Keys include the deployment that wrote them. See [Deploys](guide/deploys.md).

Two earlier designs were dropped:

- **Hashing each function's code into its keys.** It missed changes in called functions and returned models, and emptied the cache on harmless refactors and Python upgrades.
- **A version you set (`version=GIT_SHA`).** It depended on you, and redeploying the same code didn't empty the cache.

A deployment ID comes from the platform, changes on every deploy, and stays the same across restarts within a deploy, which is what "a deploy empties the cache" means. Scoping, rather than flushing, keeps old processes from refilling the cache during a rolling deploy. Tags are not scoped, so invalidations cross deployments.

`clear()` doesn't delete: every entry depends on a namespace generation counter, and clearing increments it. Retired deployments are deleted by key prefix, once no process of theirs has sent a heartbeat for `retire_after`.

## Dedupe is per process

Dedupe and batching coordinate tasks in one event loop (and, for sync functions, threads in one process). They don't coordinate workers. Across processes, a cold key can be computed once per process. Stale-while-revalidate helps for hot keys. Distributed locks are not implemented.

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

## Two engines

The async and sync engines share everything but the calling code: declarations, checks, keys, tags, sessions and the entry format. The async engine shares executions between tasks, and runs background refreshes as tasks. The sync engine shares executions between threads with locks and events, and runs refreshes in a thread pool.

Batching is async only. It merges calls that are waiting at the same moment, and sync code makes one call at a time.
