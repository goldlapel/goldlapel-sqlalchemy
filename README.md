# goldlapel-sqlalchemy

Gold Lapel plugin for SQLAlchemy — automatic Postgres query optimization with one import change. Includes L1 native cache — an in-process cache that serves repeated reads in microseconds with no TCP round-trip.

## Quick Start

### Sync

```python
# Before
from sqlalchemy import create_engine
engine = create_engine("postgresql://user:pass@host:5432/mydb")

# After
from goldlapel_sqlalchemy import create_engine
engine = create_engine("postgresql://user:pass@host:5432/mydb")
```

### Async (FastAPI)

```python
from goldlapel_sqlalchemy import create_async_engine

engine = create_async_engine("postgresql+asyncpg://user:pass@host:5432/mydb")
```

Full FastAPI example:

```python
from fastapi import FastAPI
from goldlapel_sqlalchemy import create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker

engine = create_async_engine("postgresql+asyncpg://user:pass@host:5432/mydb")
SessionLocal = async_sessionmaker(engine)

app = FastAPI()

@app.get("/users")
async def list_users():
    async with SessionLocal() as session:
        result = await session.execute(text("SELECT * FROM users"))
        return result.mappings().all()
```

### Alternative: `init()`

If you create your engine elsewhere, use `init()` to rewrite `DATABASE_URL`:

```python
import goldlapel_sqlalchemy

goldlapel_sqlalchemy.init()  # rewrites DATABASE_URL env var

# Now create your engine as usual — it connects through Gold Lapel
from sqlalchemy import create_engine
engine = create_engine(os.environ["DATABASE_URL"])
```

## Options

Pass Gold Lapel options as keyword arguments:

```python
engine = create_engine(
    "postgresql://user:pass@host:5432/mydb",
    goldlapel_config={"mode": "butler", "pool_size": 30},
    goldlapel_port=9000,
    goldlapel_extra_args=["--threshold-duration-ms", "200"],
)
```

Or with `init()`:

```python
goldlapel_sqlalchemy.init(
    config={"mode": "butler", "pool_size": 30},
    port=9000,
    extra_args=["--threshold-duration-ms", "200"],
)
```

The `goldlapel_config` dict (or `config` in `init()`) accepts any Gold Lapel configuration keys as snake_case Python dict keys. These are passed directly to the Gold Lapel proxy at startup.

## L1 Native Cache

L1 cache is enabled by default for sync engines. Every connection from the pool is wrapped with `goldlapel.wrap()`, which provides an in-process cache that serves repeated reads in microseconds with no TCP round-trip. Cache invalidation is handled automatically via the proxy's invalidation channel.

To disable L1 cache:

```python
engine = create_engine(
    "postgresql://user:pass@host:5432/mydb",
    goldlapel_l1_cache=False,
)
```

To set a custom invalidation port:

```python
engine = create_engine(
    "postgresql://user:pass@host:5432/mydb",
    goldlapel_invalidation_port=8888,
)
```

The invalidation port defaults to `proxy_port + 2` (7934 when using the default proxy port). You can also set it via `goldlapel_config={"invalidation_port": 8888}`.

If you provide a custom `creator` callable, it will be wrapped with L1 cache automatically:

```python
engine = create_engine(
    "postgresql://user:pass@host:5432/mydb",
    creator=my_connection_factory,  # your connections get L1 cache too
)
```

## Dialect Suffixes

SQLAlchemy dialect suffixes (`+asyncpg`, `+psycopg`, `+pg8000`) are handled automatically — pass your normal SQLAlchemy URL and the plugin strips the suffix for the proxy, then restores it in the proxy URL.

## Requirements

- Python 3.9+
- SQLAlchemy 1.4+
- PostgreSQL (TCP connections only — Unix sockets not supported)

## Install

```bash
pip install goldlapel-sqlalchemy
```

## Links

- [Gold Lapel](https://goldlapel.com)
- [GitHub](https://github.com/goldlapel/goldlapel-sqlalchemy)
- [PyPI](https://pypi.org/project/goldlapel-sqlalchemy/)
