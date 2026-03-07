import os
import re

import goldlapel
from sqlalchemy import create_engine as _sa_create_engine

_DIALECT_RE = re.compile(r'^(postgres(?:ql)?)\+(\w+)(://)')


def _strip_dialect(url):
    m = _DIALECT_RE.match(url)
    if m:
        return _DIALECT_RE.sub(r'\1\3', url), m.group(2)
    return url, None


def _restore_dialect(proxy_url, dialect):
    if dialect:
        return re.sub(r'^(postgres(?:ql)?)(://)', rf'\1+{dialect}\2', proxy_url)
    return proxy_url


def _start_proxy(url, kwargs):
    port = kwargs.pop("goldlapel_port", None)
    extra_args = kwargs.pop("goldlapel_extra_args", None)
    clean_url, dialect = _strip_dialect(str(url))
    proxy = goldlapel.start(clean_url, port=port, extra_args=extra_args)
    return _restore_dialect(proxy, dialect)


def create_engine(url, **kwargs):
    proxy = _start_proxy(url, kwargs)
    return _sa_create_engine(proxy, **kwargs)


def create_async_engine(url, **kwargs):
    from sqlalchemy.ext.asyncio import create_async_engine as _sa_create_async_engine
    proxy = _start_proxy(url, kwargs)
    return _sa_create_async_engine(proxy, **kwargs)


def init(url=None, *, port=None, extra_args=None):
    url = url or os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("Gold Lapel: DATABASE_URL not set. Pass a URL or set DATABASE_URL.")
    clean_url, dialect = _strip_dialect(str(url))
    proxy = goldlapel.start(clean_url, port=port, extra_args=extra_args)
    proxy = _restore_dialect(proxy, dialect)
    os.environ["DATABASE_URL"] = proxy
    return proxy


start = goldlapel.start
stop = goldlapel.stop
proxy_url = goldlapel.proxy_url
GoldLapel = goldlapel.GoldLapel
DEFAULT_PORT = goldlapel.DEFAULT_PORT
