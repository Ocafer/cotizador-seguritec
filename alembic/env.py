from __future__ import annotations
import os
import sys
from logging.config import fileConfig

from sqlalchemy import create_engine, pool
from alembic import context

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import DATABASE_URL, DB_PATH

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def _get_url() -> str:
    if DATABASE_URL:
        url = DATABASE_URL
        if "sslmode=" not in url:
            sep = "&" if "?" in url else "?"
            url += f"{sep}sslmode=require"
        return url
    return f"sqlite:///{DB_PATH}"


def run_migrations_offline() -> None:
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = _get_url()
    connectable = create_engine(url, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
