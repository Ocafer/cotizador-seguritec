"""configuracion table

Revision ID: 003
Revises: 002
Create Date: 2026-06-25
"""
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE IF NOT EXISTS configuracion (
        clave TEXT PRIMARY KEY,
        valor TEXT NOT NULL DEFAULT ''
    )""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS configuracion")
