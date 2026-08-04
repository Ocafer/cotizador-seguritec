from __future__ import annotations
from database import db_fetchone, db_exec, IS_POSTGRES, psql

# Cache en memoria — evita queries a Neon en cada render().
# Se invalida cuando el admin guarda configuración vía set_setting().
_cache: dict[str, str] = {}


def get_setting(key: str, default: str = "") -> str:
    if key in _cache:
        return _cache[key]
    try:
        row = db_fetchone(psql("SELECT valor FROM configuracion WHERE clave=?"), (key,))
        if row and row["valor"] is not None:
            _cache[key] = str(row["valor"])
            return _cache[key]
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
    _cache[key] = value
