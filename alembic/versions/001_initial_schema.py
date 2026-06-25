"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-06-25
"""
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"

    if is_pg:
        op.execute("""CREATE TABLE IF NOT EXISTS counter (
            id SERIAL PRIMARY KEY,
            last_quote_no INTEGER NOT NULL DEFAULT 0
        )""")
        op.execute(
            "INSERT INTO counter(last_quote_no) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counter)"
        )
        op.execute("""CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            sku TEXT UNIQUE,
            categoria TEXT NOT NULL DEFAULT '',
            nombre TEXT NOT NULL,
            unidad TEXT NOT NULL DEFAULT 'unidad',
            precio_bs DOUBLE PRECISION NOT NULL DEFAULT 0,
            activo INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS quotes (
            id SERIAL PRIMARY KEY,
            quote_no INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            client_name TEXT NOT NULL,
            delivery_time TEXT NOT NULL,
            validity_days INTEGER NOT NULL,
            notes TEXT
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS quote_items (
            id SERIAL PRIMARY KEY,
            quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
            sku TEXT,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            qty DOUBLE PRECISION NOT NULL,
            unit_price DOUBLE PRECISION NOT NULL
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS tecnicos (
            id SERIAL PRIMARY KEY,
            nombre TEXT NOT NULL,
            telefono TEXT,
            especialidad TEXT,
            activo INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS clientes (
            id SERIAL PRIMARY KEY,
            nombre TEXT NOT NULL,
            telefono TEXT,
            direccion TEXT,
            ci_nit TEXT,
            tipo TEXT NOT NULL DEFAULT 'residencial',
            notas TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS instalaciones (
            id SERIAL PRIMARY KEY,
            quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
            fecha_instalacion DATE NOT NULL,
            tecnico TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'pendiente',
            notas_instalacion TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS instalacion_tecnicos (
            id SERIAL PRIMARY KEY,
            instalacion_id INTEGER NOT NULL REFERENCES instalaciones(id) ON DELETE CASCADE,
            tecnico_id INTEGER,
            tecnico_nombre TEXT NOT NULL
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS gastos_trabajo (
            id SERIAL PRIMARY KEY,
            quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
            categoria TEXT NOT NULL,
            descripcion TEXT NOT NULL,
            monto NUMERIC(12,2) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS instalaciones_manuales (
            id SERIAL PRIMARY KEY,
            fecha_instalacion DATE NOT NULL,
            cliente_nombre TEXT NOT NULL,
            tecnicos TEXT NOT NULL,
            descripcion TEXT NOT NULL,
            monto_cobrado NUMERIC(12,2) NOT NULL DEFAULT 0,
            gastos NUMERIC(12,2) NOT NULL DEFAULT 0,
            notas TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
    else:
        op.execute("""CREATE TABLE IF NOT EXISTS counter (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )""")
        op.execute("INSERT OR IGNORE INTO counter(key, value) VALUES('quote_no', 0)")
        op.execute("""CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT UNIQUE,
            categoria TEXT NOT NULL DEFAULT '',
            nombre TEXT NOT NULL,
            unidad TEXT NOT NULL DEFAULT 'unidad',
            precio_bs REAL NOT NULL DEFAULT 0,
            activo INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_no INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            client_name TEXT NOT NULL,
            delivery_time TEXT NOT NULL,
            validity_days INTEGER NOT NULL,
            notes TEXT
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS quote_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id INTEGER NOT NULL,
            sku TEXT,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            qty REAL NOT NULL,
            unit_price REAL NOT NULL,
            FOREIGN KEY (quote_id) REFERENCES quotes(id)
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS tecnicos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            telefono TEXT,
            especialidad TEXT,
            activo INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            telefono TEXT,
            direccion TEXT,
            ci_nit TEXT,
            tipo TEXT NOT NULL DEFAULT 'residencial',
            notas TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS instalaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id INTEGER NOT NULL,
            fecha_instalacion TEXT NOT NULL,
            tecnico TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'pendiente',
            notas_instalacion TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (quote_id) REFERENCES quotes(id)
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS instalacion_tecnicos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instalacion_id INTEGER NOT NULL,
            tecnico_id INTEGER,
            tecnico_nombre TEXT NOT NULL,
            FOREIGN KEY (instalacion_id) REFERENCES instalaciones(id)
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS gastos_trabajo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id INTEGER NOT NULL,
            categoria TEXT NOT NULL,
            descripcion TEXT NOT NULL,
            monto REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (quote_id) REFERENCES quotes(id)
        )""")
        op.execute("""CREATE TABLE IF NOT EXISTS instalaciones_manuales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_instalacion TEXT NOT NULL,
            cliente_nombre TEXT NOT NULL,
            tecnicos TEXT NOT NULL,
            descripcion TEXT NOT NULL,
            monto_cobrado REAL NOT NULL DEFAULT 0,
            gastos REAL NOT NULL DEFAULT 0,
            notas TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        )""")


def downgrade() -> None:
    for table in [
        "instalaciones_manuales",
        "gastos_trabajo",
        "instalacion_tecnicos",
        "instalaciones",
        "clientes",
        "tecnicos",
        "quote_items",
        "quotes",
        "products",
        "counter",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {table}")
