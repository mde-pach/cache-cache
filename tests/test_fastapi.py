import json
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request

from cache_cache import Cache, cached, current_session


class ProductRepo:
    calls = 0

    @cached(tags=["product:{pid}"])
    async def get(self, pid: int) -> dict[str, int]:
        ProductRepo.calls += 1
        return {"id": pid}


def build_app(cache: Cache) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        async with cache:
            yield {"cache": cache}

    async def cache_session(request: Request):  # type: ignore[no-untyped-def]
        async with request.state.cache.session(max_entries=1000):
            yield

    app = FastAPI(lifespan=lifespan, dependencies=[Depends(cache_session)])

    @app.get("/products/{pid}")
    async def read_product(pid: int, repo: ProductRepo = Depends()) -> dict[str, object]:
        first = await repo.get(pid)
        second = await repo.get(pid)
        assert first is second
        return {"product": first, "in_session": current_session() is not None}

    return app


async def test_fastapi_integration() -> None:
    ProductRepo.calls = 0
    cache = Cache()
    app = build_app(cache)
    async with app.router.lifespan_context(app) as state:
        assert cache.is_open
        r1 = await _get(app, state, "/products/1")
        r2 = await _get(app, state, "/products/1")
    assert not cache.is_open
    assert r1 == {"product": {"id": 1}, "in_session": True}
    assert r2 == r1
    assert ProductRepo.calls == 1


async def _get(app: FastAPI, state: dict[str, object], path: str) -> object:
    """Send one request through ASGI with the lifespan state, as a server would."""
    responses: list[dict[str, object]] = []
    body: list[bytes] = []
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("test", 1),
        "server": ("test", 80),
        "state": dict(state),
    }

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        responses.append(message)
        if message["type"] == "http.response.body":
            body.append(message.get("body", b""))  # type: ignore[arg-type]

    await app(scope, receive, send)
    assert responses[0]["status"] == 200, b"".join(body)
    return json.loads(b"".join(body))
