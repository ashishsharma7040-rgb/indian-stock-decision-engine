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


def _database_url_parts(raw_url: str) -> dict[str, Any]:
    if not raw_url:
        return {"host": None, "port": None, "database": None, "username_style": None, "pooler": False}
    try:
        parsed = urlsplit(raw_url)
        host = parsed.hostname
        try:
            port = parsed.port
        except ValueError:
            port = None
        username = parsed.username or ""
        database_name = parsed.path.lstrip("/") or None
        pooler = bool(host and "pooler.supabase.com" in host.lower())
        username_style = (
            "pooler_project_user" if username.startswith("postgres.") else
            "postgres_user" if username == "postgres" else
            "custom_user" if username else
            None
        )
        return {
            "host": host,
            "port": port,
            "database": database_name,
            "username_style": username_style,
            "pooler": pooler,
            "pooler_mode_hint": "transaction" if pooler and port == 6543 else "session_or_direct" if port == 5432 else None,
        }
    except Exception as exc:
        return {"host": None, "port": None, "database": None, "username_style": None, "pooler": False, "parse_error": str(exc)}


DATABASE_URL, DATABASE_URL_SOURCE = _configured_database_url()
DATABASE_URL_KIND = _database_url_kind(DATABASE_URL)
DATABASE_URL_PARTS = _database_url_parts(DATABASE_URL)
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
        command_timeout = int(os.getenv("DB_COMMAND_TIMEOUT", "120"))
        statement_timeout_ms = str(int(os.getenv("DB_STATEMENT_TIMEOUT_MS", str(command_timeout * 1000))))
        engine = create_async_engine(
            _async_database_url(DATABASE_URL),
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
            pool_pre_ping=True,
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "300")),
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
            connect_args={
                # Supabase pooler/PgBouncer compatibility: avoid asyncpg
                # prepared statement cache and give bulk bhavcopy sync enough
                # time to delete/insert thousands of EOD rows.
                "statement_cache_size": 0,
                "command_timeout": command_timeout,
                "server_settings": {
                    "statement_timeout": statement_timeout_ms,
                    "tcp_keepalives_idle": os.getenv("DB_TCP_KEEPALIVES_IDLE", "60"),
                    "tcp_keepalives_interval": os.getenv("DB_TCP_KEEPALIVES_INTERVAL", "10"),
                    "tcp_keepalives_count": os.getenv("DB_TCP_KEEPALIVES_COUNT", "3"),
                },
            },
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
    connection_warnings: list[str] = []
    if DATABASE_URL_PARTS.get("pooler") and DATABASE_URL_PARTS.get("port") == 5432:
        connection_warnings.append(
            "This looks like a Supabase session-pooler URL on port 5432. For Render, use Transaction pooler URI, usually port 6543."
        )
    host = str(DATABASE_URL_PARTS.get("host") or "")
    if host.startswith("aws-1-"):
        connection_warnings.append(
            "If this host does not resolve on Render, copy a fresh Transaction pooler URI from Supabase instead of manually editing the host."
        )
    common_fixes = [
        "Render key must be DATABASE_URL and the value field must contain only the postgresql:// URI, not DATABASE_URL=...",
        "Use Supabase Project Settings > Database > Connection string > Transaction pooler > URI.",
        "Transaction pooler URLs commonly use port 6543. Session pooler/direct URLs commonly use port 5432.",
        "If the DB password contains #, /, ?, %, spaces, or : then URL-encode it or reset it to a simple alphanumeric password.",
        "After changing Render environment variables, redeploy/restart the backend.",
    ]
    return {
        "enabled": DATABASE_ENABLED,
        "driver": "postgresql+asyncpg" if DATABASE_ENABLED else None,
        "url_configured": bool(DATABASE_URL),
        "url_source": DATABASE_URL_SOURCE,
        "url_kind": DATABASE_URL_KIND,
        "url_redacted": _redacted_database_url(DATABASE_URL),
        "host": DATABASE_URL_PARTS.get("host"),
        "port": DATABASE_URL_PARTS.get("port"),
        "database": DATABASE_URL_PARTS.get("database"),
        "username_style": DATABASE_URL_PARTS.get("username_style"),
        "pooler": DATABASE_URL_PARTS.get("pooler"),
        "pooler_mode_hint": DATABASE_URL_PARTS.get("pooler_mode_hint"),
        "error": None if DATABASE_ENABLED else DATABASE_ERROR,
        "connection_warnings": connection_warnings,
        "expected_variable": "DATABASE_URL",
        "fallback_variables": ["SUPABASE_DATABASE_URL", "POSTGRES_URL"],
        "requires_supabase_api_key": False,
        "common_connection_fixes": common_fixes,
        "supabase_api_url_configured": bool(os.getenv("SUPABASE_URL", "").strip()),
        "supabase_api_key_configured": bool(
            os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip() or os.getenv("SUPABASE_ANON_KEY", "").strip()
        ),
        "note": (
            "This backend connects to Supabase through Postgres/SQLAlchemy. It does not need "
            "SUPABASE_URL, ANON_KEY, or SERVICE_ROLE_KEY unless you later add Supabase REST client code."
        ),
    }
