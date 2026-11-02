"""Benchmark target service.

The JSON serializer is chosen with the SERIALIZER environment variable:
  default -> Starlette's JSONResponse (standard-library json)
  orjson  -> orjson

Handlers return Response objects directly. That skips FastAPI's
jsonable_encoder step, so the only thing that differs between the two
serializer settings is the serializer itself.
"""
import asyncio
import os
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response

SERIALIZER = os.environ.get("SERIALIZER", "default")
IO_DELAY_S = 0.005  # simulated I/O wait: 5 ms

if SERIALIZER == "orjson":
    import orjson

    class FastJSONResponse(Response):
        media_type = "application/json"

        def render(self, content) -> bytes:
            return orjson.dumps(content)

    RESP = FastJSONResponse
elif SERIALIZER == "default":
    RESP = JSONResponse
else:
    raise RuntimeError(f"unknown SERIALIZER={SERIALIZER!r} (use 'default' or 'orjson')")

# Small payload: a single object (about 70 bytes of JSON).
SMALL = {"id": 1, "name": "widget", "price": 9.99, "tags": ["a", "b"], "active": True}

# Large payload: 1000 objects (roughly 90 KB of JSON). Built once at import
# time, serialized on every request.
LARGE = [
    {
        "id": i,
        "name": f"item-{i}",
        "price": round(i * 1.37, 2),
        "tags": ["alpha", "beta", "gamma"],
        "active": i % 2 == 0,
    }
    for i in range(1000)
]

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


# --- Used by the runner to confirm how many workers are answering ----------

@app.get("/pid")
async def pid():
    return Response(str(os.getpid()), media_type="text/plain")


# --- Handler type x payload size -------------------------------------------

@app.get("/small-sync")
def small_sync():
    return RESP(SMALL)


@app.get("/small-async")
async def small_async():
    return RESP(SMALL)


@app.get("/large-sync")
def large_sync():
    return RESP(LARGE)


@app.get("/large-async")
async def large_async():
    return RESP(LARGE)


# --- Waiting on I/O: three ways to write the same 5 ms wait ----------------

@app.get("/io-async")
async def io_async():
    # Non-blocking: the event loop serves other requests during the wait.
    await asyncio.sleep(IO_DELAY_S)
    return RESP(SMALL)


@app.get("/io-sync")
def io_sync():
    # Blocking call in a sync handler: runs in Starlette's thread pool.
    time.sleep(IO_DELAY_S)
    return RESP(SMALL)


@app.get("/io-blocking-async")
async def io_blocking_async():
    # Anti-pattern, included on purpose: a blocking call inside async def
    # stalls the whole event loop for the duration of the wait.
    time.sleep(IO_DELAY_S)
    return RESP(SMALL)
