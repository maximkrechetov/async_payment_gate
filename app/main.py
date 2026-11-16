from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse

from app.api import health, payments
from app.core.config import settings
from app.core.exceptions import IdempotencyConflictError, NotFoundError
from app.db.session import engine


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Payment Processing Service",
        version="1.0.0",
        debug=settings.debug,
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )

    app.include_router(health.router)
    app.include_router(payments.router, prefix=settings.api_prefix)

    @app.exception_handler(NotFoundError)
    async def _not_found(_: Request, exc: NotFoundError) -> ORJSONResponse:
        return ORJSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(IdempotencyConflictError)
    async def _conflict(_: Request, exc: IdempotencyConflictError) -> ORJSONResponse:
        return ORJSONResponse(status_code=409, content={"detail": str(exc)})

    return app


app = create_app()
