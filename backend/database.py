from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
        converted = raw_url
    elif raw_url.startswith("postgres://"):
        converted = raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif raw_url.startswith("postgresql://"):
        converted = raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    else:
        return raw_url

    parsed = urlsplit(converted)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    sslmode = params.pop("sslmode", None)
    if sslmode and "ssl" not in params:
        params["ssl"] = "require" if sslmode in {"require", "verify-ca", "verify-full"} else sslmode
    host = (parsed.hostname or "").lower()
    if ("supabase" in host or "pooler" in host) and "ssl" not in params:
        params["ssl"] = "require"
    if ("pooler" in host or parsed.port == 6543) and "prepared_statement_cache_size" not in params:
        params["prepared_statement_cache_size"] = "0"
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(params), parsed.fragment))


def _configured_database_url() -> tuple[str, str | None]:
    for key in ("DATABASE_URL", "SUPABASE_DATABASE_URL", "POSTGRES_URL"):
        value = os.getenv(key, "").strip()
        if value:
            return value, key
    return "", None


def _database_url_kind(raw_url: str) -> str:
    if not raw_url:
        return "missing"
    lowered = raw_url.lower()
    if lowered.startswith(("https://", "http://")):
        return "supabase_api_url"
    if lowered.startswith(("postgresql://", "postgres://", "postgresql+asyncpg://")):
        return "postgresql"
    return "unsupported"


def _redacted_database_url(raw_url: str) -> str | None:
    if not raw_url:
        return None
    try:
        parsed = urlsplit(raw_url)
        if not parsed.scheme:
            return "unrecognized-url"
        host = parsed.hostname or "unknown-host"
        port = f":{parsed.port}" if parsed.port else ""
        path = parsed.path or ""
        credentials = "***:***@" if parsed.username or parsed.password else ""
        return f"{parsed.scheme}://{credentials}{host}{port}{path}"
    except Exception:
        return f"{raw_url[:12]}..."


DATABASE_URL, DATABASE_URL_SOURCE = _configured_database_url()
DATABASE_URL_KIND = _database_url_kind(DATABASE_URL)
DATABASE_ERROR: str | None = None
Base = declarative_base()
engine = None
AsyncSessionLocal = None
DATABASE_ENABLED = False

if not DATABASE_URL:
    DATABASE_ERROR = "No Postgres URL is configured. Set DATABASE_URL to the Supabase Postgres connection string."
elif DATABASE_URL_KIND == "supabase_api_url":
    DATABASE_ERROR = (
        "DATABASE_URL is a Supabase API/web URL. Use the Supabase Postgres connection string "
        "from Project Settings > Database, not https://<project>.supabase.co."
    )
elif DATABASE_URL_KIND != "postgresql":
    DATABASE_ERROR = "DATABASE_URL must start with postgresql://, postgres://, or postgresql+asyncpg://."
elif not create_async_engine or not async_sessionmaker:
    DATABASE_ERROR = "SQLAlchemy async dependencies are not installed."
else:
    try:
        engine = create_async_engine(
            _async_database_url(DATABASE_URL),
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
            pool_pre_ping=True,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
        )
        AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        DATABASE_ENABLED = True
    except Exception as exc:  # pragma: no cover - deployment configuration dependent
        DATABASE_ERROR = str(exc)
        engine = None
        AsyncSessionLocal = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        raise RuntimeError(DATABASE_ERROR or "DATABASE_URL is not configured")
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> dict[str, Any]:
    if not DATABASE_ENABLED or engine is None:
        return {"enabled": False, "status": "disabled", "reason": DATABASE_ERROR or "DATABASE_URL is not configured"}
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return {"enabled": True, "status": "ready"}


def database_status() -> dict[str, Any]:
    return {
        "enabled": DATABASE_ENABLED,
        "driver": "postgresql+asyncpg" if DATABASE_ENABLED else None,
        "url_configured": bool(DATABASE_URL),
        "url_source": DATABASE_URL_SOURCE,
        "url_kind": DATABASE_URL_KIND,
        "url_redacted": _redacted_database_url(DATABASE_URL),
        "error": None if DATABASE_ENABLED else DATABASE_ERROR,
        "expected_variable": "DATABASE_URL",
        "fallback_variables": ["SUPABASE_DATABASE_URL", "POSTGRES_URL"],
        "requires_supabase_api_key": False,
        "supabase_api_url_configured": bool(os.getenv("SUPABASE_URL", "").strip()),
        "supabase_api_key_configured": bool(
            os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip() or os.getenv("SUPABASE_ANON_KEY", "").strip()
        ),
        "note": (
            "This backend connects to Supabase through Postgres/SQLAlchemy. It does not need "
            "SUPABASE_URL, ANON_KEY, or SERVICE_ROLE_KEY unless you later add Supabase REST client code."
        ),
    }
