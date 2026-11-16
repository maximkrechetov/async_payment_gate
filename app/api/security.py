import secrets
from typing import Annotated

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import settings

api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    provided: Annotated[str | None, Security(api_key_scheme)] = None,
) -> None:
    if provided is None or not secrets.compare_digest(provided, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-API-Key is invalid",
            headers={"WWW-Authenticate": "APIKey"},
        )
