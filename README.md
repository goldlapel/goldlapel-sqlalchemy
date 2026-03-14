# goldlapel-sqlalchemy

Gold Lapel plugin for SQLAlchemy — automatic Postgres query optimization with one import change.

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
