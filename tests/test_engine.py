import os
from unittest.mock import MagicMock, patch

import pytest

from goldlapel_sqlalchemy import (
    _strip_dialect,
    _restore_dialect,
    create_engine,
    create_async_engine,
    init,
)
import goldlapel_sqlalchemy


PROXY_URL = "postgresql://localhost:7932/mydb"


class TestStripDialect:
    def test_strips_asyncpg(self):
        url, dialect = _strip_dialect("postgresql+asyncpg://user:pass@host:5432/db")
        assert url == "postgresql://user:pass@host:5432/db"
        assert dialect == "asyncpg"

    def test_strips_psycopg(self):
        url, dialect = _strip_dialect("postgresql+psycopg://user:pass@host:5432/db")
        assert url == "postgresql://user:pass@host:5432/db"
        assert dialect == "psycopg"

    def test_plain_postgresql_unchanged(self):
        url, dialect = _strip_dialect("postgresql://user:pass@host:5432/db")
        assert url == "postgresql://user:pass@host:5432/db"
        assert dialect is None

    def test_plain_postgres_unchanged(self):
        url, dialect = _strip_dialect("postgres://user:pass@host:5432/db")
        assert url == "postgres://user:pass@host:5432/db"
        assert dialect is None


class TestRestoreDialect:
    def test_restores_asyncpg(self):
        result = _restore_dialect("postgresql://localhost:7932/db", "asyncpg")
        assert result == "postgresql+asyncpg://localhost:7932/db"

    def test_noop_when_dialect_is_none(self):
        result = _restore_dialect("postgresql://localhost:7932/db", None)
        assert result == "postgresql://localhost:7932/db"


class TestCreateEngine:
    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("goldlapel_sqlalchemy._sa_create_engine")
    def test_starts_proxy_and_returns_engine(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_sa.return_value = MagicMock()

        engine = create_engine("postgresql://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        mock_sa.assert_called_once_with(PROXY_URL)
        assert engine is mock_sa.return_value

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("goldlapel_sqlalchemy._sa_create_engine")
    def test_strips_and_restores_dialect(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL

        create_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        mock_sa.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("goldlapel_sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_port(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL

        create_engine("postgresql://host/db", goldlapel_port=9000)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=None, port=9000, extra_args=None
        )
        # goldlapel_port must not leak to SQLAlchemy
        mock_sa.assert_called_once_with(PROXY_URL)

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("goldlapel_sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_extra_args(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        extra = ["--threshold-duration-ms", "200"]

        create_engine("postgresql://host/db", goldlapel_extra_args=extra)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=None, port=None, extra_args=extra
        )
        mock_sa.assert_called_once_with(PROXY_URL)

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("goldlapel_sqlalchemy._sa_create_engine")
    def test_pops_goldlapel_config(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        cfg = {"mode": "butler", "pool_size": 30}

        create_engine("postgresql://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=cfg, port=None, extra_args=None
        )
        # goldlapel_config must not leak to SQLAlchemy
        mock_sa.assert_called_once_with(PROXY_URL)

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("goldlapel_sqlalchemy._sa_create_engine")
    def test_passes_remaining_kwargs(self, mock_sa, mock_gl):
        mock_gl.start.return_value = PROXY_URL

        create_engine("postgresql://host/db", echo=True, pool_size=5)

        mock_sa.assert_called_once_with(PROXY_URL, echo=True, pool_size=5)


class TestCreateAsyncEngine:
    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_starts_proxy_and_returns_async_engine(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        mock_sa_async.return_value = MagicMock()

        engine = create_async_engine("postgresql+asyncpg://user:pass@host:5432/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")
        assert engine is mock_sa_async.return_value

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_pops_goldlapel_config(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        cfg = {"mode": "butler", "pool_size": 30}

        create_async_engine("postgresql+asyncpg://host/db", goldlapel_config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=cfg, port=None, extra_args=None
        )
        # goldlapel_config must not leak to SQLAlchemy
        mock_sa_async.assert_called_once_with("postgresql+asyncpg://localhost:7932/mydb")

    @patch("goldlapel_sqlalchemy.goldlapel")
    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    def test_passes_remaining_kwargs(self, mock_sa_async, mock_gl):
        mock_gl.start.return_value = PROXY_URL

        create_async_engine("postgresql+asyncpg://host/db", echo=True, pool_size=5)

        mock_sa_async.assert_called_once_with(
            "postgresql+asyncpg://localhost:7932/mydb", echo=True, pool_size=5
        )


class TestInit:
    @patch("goldlapel_sqlalchemy.goldlapel")
    def test_rewrites_database_url(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host:5432/db")
        mock_gl.start.return_value = PROXY_URL

        init()

        assert os.environ["DATABASE_URL"] == PROXY_URL

    @patch("goldlapel_sqlalchemy.goldlapel")
    def test_explicit_url_over_env(self, mock_gl, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://old@host/db")
        mock_gl.start.return_value = PROXY_URL

        init(url="postgresql://new@host/db")

        mock_gl.start.assert_called_once_with(
            "postgresql://new@host/db", config=None, port=None, extra_args=None
        )

    def test_raises_when_no_url(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(ValueError, match="DATABASE_URL not set"):
            init()

    @patch("goldlapel_sqlalchemy.goldlapel")
    def test_returns_proxy_url(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL

        result = init(url="postgresql://host/db")

        assert result == PROXY_URL

    @patch("goldlapel_sqlalchemy.goldlapel")
    def test_preserves_dialect_suffix(self, mock_gl, monkeypatch):
        mock_gl.start.return_value = PROXY_URL
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@host:5432/db")

        init()

        mock_gl.start.assert_called_once_with(
            "postgresql://user:pass@host:5432/db", config=None, port=None, extra_args=None
        )
        assert os.environ["DATABASE_URL"] == "postgresql+asyncpg://localhost:7932/mydb"

    @patch("goldlapel_sqlalchemy.goldlapel")
    def test_passes_config(self, mock_gl):
        mock_gl.start.return_value = PROXY_URL
        cfg = {"mode": "butler", "pool_size": 30}

        init(url="postgresql://host/db", config=cfg)

        mock_gl.start.assert_called_once_with(
            "postgresql://host/db", config=cfg, port=None, extra_args=None
        )


class TestReExports:
    def test_start(self):
        assert goldlapel_sqlalchemy.start is goldlapel_sqlalchemy.goldlapel.start

    def test_stop(self):
        assert goldlapel_sqlalchemy.stop is goldlapel_sqlalchemy.goldlapel.stop

    def test_proxy_url(self):
        assert goldlapel_sqlalchemy.proxy_url is goldlapel_sqlalchemy.goldlapel.proxy_url

    def test_goldlapel_class(self):
        assert goldlapel_sqlalchemy.GoldLapel is goldlapel_sqlalchemy.goldlapel.GoldLapel

    def test_default_port(self):
        assert goldlapel_sqlalchemy.DEFAULT_PORT is goldlapel_sqlalchemy.goldlapel.DEFAULT_PORT
