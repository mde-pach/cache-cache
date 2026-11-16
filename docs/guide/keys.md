# Keys

You don't write keys. A key is built from:

1. the cache's `namespace` and `version`,
2. the function's module and qualified name,
3. the function's `version=`,
4. a digest of its arguments.

See [Deploys and clearing](deploys.md) for the versions.

`self` is never part of a key. Defaults are applied first, so `get(1)` and `get(1, include_deleted=False)` share an entry.

## Deterministic encoding

Keys are shared between processes, so they can't use `hash()`. Each argument is encoded into a canonical structure instead. Supported out of the box:

- `None`, `bool`, `int`, `float`, `str`, `bytes`
- `uuid.UUID`, `datetime`, `date`, `time`, `timedelta`, `Decimal`
- enums
- `list`, `tuple`, `set`, `frozenset`, `dict` and other mappings (sets and dicts are ordered canonically)
- dataclass instances and pydantic models

Types never collide: `1`, `"1"`, `1.0` and `True` give different keys.

A parameter annotated with a type that has no encoding fails when the cache opens, not on the first call:

```text
ConfigurationError: myapp.repo.ProductRepo.search: parameter 'db' of type <class 'AsyncSession'>
has no key encoding. Add the type to key_ignore or key_formats, or write a key function
with @search.key
```

Parameters that are unannotated or annotated `Any` are checked per call instead.

## `key_ignore` and `key_formats`

Set them on the cache for every function, or on one decorator on top of the cache's rules:

```python
from decimal import Decimal

cache = Cache(
    key_ignore=[AsyncSession, Request],                                  # never part of a key
    key_formats={Decimal: lambda d: str(d.quantize(Decimal("0.00")))},  # how a type appears
)

@cached(key_ignore=[Locale])
async def search(self, query: SearchQuery, locale: Locale) -> list[Product]: ...
```

- A format applies to subclasses too, and must return a different type than its input.
- Ignored parameters are matched by their annotation, and also by the runtime type of the value.

## Key functions

When the default isn't right, register a function with the same signature:

```python
class ProductRepo:
    @cached
    async def search(self, query: SearchQuery, trace_id: str) -> list[Product]: ...

    @search.key
    def _search_key(self, query: SearchQuery, trace_id: str) -> tuple[str, int]:
        return (query.text.lower(), query.page)
```

- A key function with different parameter names raises `ConfigurationError` at import.
- The value it returns is encoded with the same rules as arguments.

## Instances { #instances }

If instances of a class aren't interchangeable (one API client per tenant, per base URL…), define `__cache_key__`:

```python
class FxClient:
    def __init__(self, tenant: str) -> None:
        self.tenant = tenant

    def __cache_key__(self) -> str:
        return self.tenant

    @cached(ttl=60)
    async def rates(self) -> dict[str, float]: ...
```

Its return value is encoded and added to every key of that instance's methods.

## Conditions

```python
class ProductRepo:
    @cached(ttl=600, tags=["product:list:{category}"])
    async def by_category(self, category: str) -> list[Product]: ...

    @by_category.condition                   # same signature: use the cache for this call?
    def _cacheable(self, category: str) -> bool:
        return category != "flash-sale"

    @by_category.result_condition            # (self, result): keep this result?
    def _worth_keeping(self, result: list[Product]) -> bool:
        return len(result) > 0
```

- A call whose condition is false runs the function directly. It isn't deduped, but batching still applies.
- A result rejected by `result_condition` is returned to the caller, and to callers that joined the same call, but it isn't stored.

!!! warning "Don't name a method `list`"
    Inside a class body, `def list(...)` shadows the builtin for the rest of the body. On Python versions before 3.14, an annotation like `result: list[Product]` that comes after it then fails, unless the module uses `from __future__ import annotations`.
