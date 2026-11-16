from collections import defaultdict

from fastapi import FastAPI, Request, Response

app = FastAPI(title="Demo webhook receiver")
_hits: dict[str, int] = defaultdict(int)


@app.post("/hook")
async def hook(request: Request) -> dict[str, bool]:
    print(
        f"[WEBHOOK OK] {request.headers.get('X-Signature')} {(await request.body()).decode()}",
        flush=True,
    )
    return {"ok": True}


@app.post("/flaky")
async def flaky(request: Request) -> Response:
    payment_id = request.headers.get("X-Payment-Id", "unknown")
    _hits[payment_id] += 1
    attempt = _hits[payment_id]
    print(f"[WEBHOOK FLAKY] attempt={attempt} payment={payment_id}", flush=True)
    if attempt < 3:
        return Response(status_code=500, content="temporary error")
    return Response(status_code=200, content="ok")


@app.post("/fail")
async def fail() -> Response:
    return Response(status_code=500, content="always fails")
