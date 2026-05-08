"""PostgreSQL connection pool via SQLAlchemy."""
from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from src.utils.config import get_settings


class Base(DeclarativeBase):
    pass


@lru_cache(maxsize=1)
def get_engine():
    cfg = get_settings().database
    return create_engine(
        cfg.url,
        pool_size=cfg.pool_size,
        max_overflow=cfg.max_overflow,
        echo=cfg.echo,
        pool_pre_ping=True,
    )


@lru_cache(maxsize=1)
def get_session_factory():
    return sessionmaker(bind=get_engine(), expire_on_commit=False)


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Context manager that yields a session and auto-commits / rolls back."""
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def health_check() -> bool:
    """Return True if DB is reachable."""
    try:
        with get_db_session() as s:
            s.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
