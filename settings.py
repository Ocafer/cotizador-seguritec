from __future__ import annotations
from database import db_fetchone, db_exec, IS_POSTGRES, psql


def get_setting(key: str, default: str = "") -> str:
    try:
        row = db_fetchone(psql("SELECT valor FROM configuracion WHERE clave=?"), (key,))
        if row and row["valor"] is not None:
            return str(row["valor"])
    except Exception:
        pass
    return default


def set_setting(key: str, value: str) -> None:
    if IS_POSTGRES:
        db_exec(
            "INSERT INTO configuracion(clave,valor) VALUES(%s,%s) ON CONFLICT(clave) DO UPDATE SET valor=EXCLUDED.valor",
            (key, value),
        )
    else:
        db_exec(
            "INSERT OR REPLACE INTO configuracion(clave,valor) VALUES(?,?)",
            (key, value),
        )
