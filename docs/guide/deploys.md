# Deploys

A cached entry outlives the code that wrote it, and sometimes the data it was computed from. cache-cache empties the cache on every deploy, automatically, including a redeploy of the same code, without breaking rolling deploys.

## The problem with flushing

The usual fix is to flush the cache after a deploy (`FLUSHDB` in a post-deploy hook). With rolling deploys it doesn't work. While new processes start, old ones keep serving traffic and refill the cache with the old data right after the flush. A crash restart in the middle of the day doesn't need a flush, yet a restart-based flush would empty the cache for everyone.

## Entries belong to a deployment

Every key contains the deployment it was written by, the way Next.js puts its `deploymentId` in `'use cache'` keys:

```python
cache = Cache()                        # deployment="auto"
```

- **A new deployment reads nothing from the previous one,** on every process at once. That includes redeploying the same commit.
- **Old processes can't pollute the new deployment.** During a rollout, they write into their own scope.
- **A restart keeps the scope:** a crash, a scale-up or a new worker in the same deployment shares the entries.
- **Tags are shared by every deployment.** An invalidation from a new process reaches the entries still used by old processes, and the other way round.

### Where the ID comes from

`deployment="auto"` reads the first of these that is set:

| Variable | Platform | Changes on |
|---|---|---|
| `CACHE_CACHE_DEPLOYMENT` | you | whatever you set |
| `VERCEL_DEPLOYMENT_ID` | Vercel | every deployment |
| `RAILWAY_DEPLOYMENT_ID` | Railway | every deployment |
| `HEROKU_RELEASE_VERSION` | Heroku, with [dyno metadata](https://devcenter.heroku.com/articles/dyno-metadata) enabled | every release, including config changes |
| `FLY_IMAGE_REF` | Fly.io (`…:deployment-01H9…`) | every `fly deploy` |
| `K_REVISION` | Cloud Run, Knative | every revision |

Elsewhere, set `CACHE_CACHE_DEPLOYMENT` to a value that changes on every deploy: your CI's run or deployment ID, for example. Don't use the commit SHA if you want a redeploy of the same commit to empty the cache.

On Kubernetes, expose the ReplicaSet's `pod-template-hash` label. It's the same for every pod of a rollout, survives pod restarts, and changes on every rollout, including `kubectl rollout restart`:

```yaml
env:
  - name: CACHE_CACHE_DEPLOYMENT
    valueFrom:
      fieldRef:
        fieldPath: metadata.labels['pod-template-hash']
```

### When no ID is found

The cache logs a warning and scopes entries to the process. That keeps the guarantee that a deploy never reads old entries, but processes don't share entries: with four workers, each one fills its own. Set an ID to share them.

### Other settings

```python
Cache(deployment="2026-11-18.3")   # an explicit ID
Cache(deployment="process")        # one scope per process, without the warning
Cache(deployment=None)             # not scoped: entries survive deploys
```

With `deployment=None`, entries survive deploys until they expire or are invalidated. Use the function-level `version=` to stop reading one function's old entries:

```python
@cached(ttl=600, version="2")
async def by_category(self, category: str) -> list[Product]: ...
```

## Deleting old deployments

Scoping makes old entries unreadable. Deleting them frees the memory:

1. Every process sends a heartbeat for its deployment every `heartbeat_interval` (60 s). In Redis, it's a sorted set timed with the Redis server's clock, so host clocks don't matter.
2. After each heartbeat, a process looks for deployments of its namespace with no heartbeat for `retire_after` (10 min).
3. It deletes their entries (`SCAN` and `UNLINK` in Redis), behind a lock so that one process does it, and emits a `retire` event.

A rolling deploy then goes like this:

| Time | What happens |
|---|---|
| t₀ | New pods start in deployment B and find no entries. Old pods keep serving from A. |
| rollout | Invalidations from A or B reach both. |
| t₁ | The last A pod stops. A's heartbeats stop. |
| t₁ + 10 min | A B process deletes every A entry. |

Pick `retire_after` longer than the longest gap between heartbeats of a deployment that's still running: a paused canary, a scaled-to-zero app. A deployment that comes back after being collected just starts empty. `await cache.collect_retired()` collects immediately.

Deletion needs a backend with a deployment registry. `MemoryBackend`, `RedisBackend` and their sync versions have one. With other backends, old entries just expire with their ttl.

## What it costs

- **A cold cache after each deploy.** Expect more load while it fills. Dedupe limits it to about one computation per key and process, and you can warm hot keys at startup with `prime()` or plain calls.
- **One heartbeat per process per minute**, and a registry lookup after it.

## `clear()`

```python
await cache.clear()      # SyncCache: cache.clear()
```

Empties the namespace now, for every deployment and every process sharing the backend. It's a single write: every entry depends on a namespace generation counter, and `clear()` increments it. Use it to recover from bad data without a deploy. Cleared entries are never served as stale values.
