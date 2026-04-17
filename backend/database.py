from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from typing import Any

try:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from sqlalchemy.orm import declarative_base
except Exception:  # pragma: no cover - optional local zero-install mode
    AsyncSession = Any  # type: ignore
    async_sessionmaker = None  # type: ignore
    create_async_engine = None  # type: ignore

    def declarative_base() -> Any:  # type: ignore
        class _Base:
            metadata = None

        return _Base


def _async_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgresql+asyncpg://"):
        return raw_url
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return raw_url


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATABASE_ENABLED = bool(DATABASE_URL and create_async_engine and async_sessionmaker)
Base = declarative_base()
engine = None
AsyncSessionLocal = None

if DATABASE_ENABLED:
    engine = create_async_engine(
        _async_database_url(DATABASE_URL),
        echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        pool_pre_ping=True,
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
    )
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        raise RuntimeError("DATABASE_URL is not configured")
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> dict[str, Any]:
    if not DATABASE_ENABLED or engine is None:
        return {"enabled": False, "status": "disabled", "reason": "DATABASE_URL is not configured"}
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return {"enabled": True, "status": "ready"}


def database_status() -> dict[str, Any]:
    return {
        "enabled": DATABASE_ENABLED,
        "driver": "postgresql+asyncpg" if DATABASE_ENABLED else None,
        "url_configured": bool(DATABASE_URL),
    }
