from __future__ import annotations
from typing import List

from config import IVA_RATE
from database import db_fetchall, psql
from models import (
    Product, ProductoRow, TecnicoRow, ClienteRow,
    GastoRow, InstalacionManualRow,
)


def load_products() -> List[Product]:
    rows = db_fetchall("""
        SELECT sku, categoria, nombre, unidad, precio_bs, activo
        FROM products WHERE activo=1 ORDER BY categoria, nombre
    """)
    return [Product(
        sku=(r["sku"] or "").strip() if r["sku"] else "",
        categoria=str(r["categoria"] or ""),
        nombre=str(r["nombre"] or ""),
        unidad=str(r["unidad"] or "unidad"),
        precio_bs=float(r["precio_bs"] or 0),
    ) for r in rows]


def load_all_products() -> List[ProductoRow]:
    rows = db_fetchall(
        "SELECT id, sku, categoria, nombre, unidad, precio_bs, activo FROM products ORDER BY categoria, nombre"
    )
    return [ProductoRow(
        id=int(r["id"]), sku=str(r["sku"] or ""),
        categoria=str(r["categoria"] or ""), nombre=str(r["nombre"] or ""),
        unidad=str(r["unidad"] or "unidad"), precio_bs=float(r["precio_bs"] or 0),
        activo=int(r["activo"]),
    ) for r in rows]


def load_tecnicos_activos() -> List[TecnicoRow]:
    rows = db_fetchall(
        "SELECT id, nombre, telefono, especialidad, activo FROM tecnicos WHERE activo=1 ORDER BY nombre"
    )
    return [TecnicoRow(
        id=int(r["id"]), nombre=str(r["nombre"]),
        telefono=str(r["telefono"] or ""), especialidad=str(r["especialidad"] or ""),
        activo=1,
    ) for r in rows]


def load_all_tecnicos() -> List[TecnicoRow]:
    rows = db_fetchall("""
        SELECT t.id, t.nombre, t.telefono, t.especialidad, t.activo,
               COUNT(it.id) AS total_instalaciones
        FROM tecnicos t LEFT JOIN instalacion_tecnicos it ON it.tecnico_id = t.id
        GROUP BY t.id, t.nombre, t.telefono, t.especialidad, t.activo ORDER BY t.nombre
    """)
    return [TecnicoRow(
        id=int(r["id"]), nombre=str(r["nombre"]),
        telefono=str(r["telefono"] or ""), especialidad=str(r["especialidad"] or ""),
        activo=int(r["activo"]), total_instalaciones=int(r["total_instalaciones"]),
    ) for r in rows]


def load_all_clientes() -> List[ClienteRow]:
    rows = db_fetchall(
        "SELECT id, nombre, telefono, direccion, ci_nit, tipo, notas FROM clientes ORDER BY nombre"
    )
    return [ClienteRow(
        id=int(r["id"]), nombre=str(r["nombre"]),
        telefono=str(r["telefono"] or ""), direccion=str(r["direccion"] or ""),
        ci_nit=str(r["ci_nit"] or ""), tipo=str(r["tipo"] or "residencial"),
        notas=str(r["notas"] or ""),
    ) for r in rows]


def get_quote_total(quote_id: int) -> float:
    rows = db_fetchall(psql("SELECT qty, unit_price FROM quote_items WHERE quote_id=?"), (quote_id,))
    subtotal = sum(float(r["qty"]) * float(r["unit_price"]) for r in rows)
    return subtotal * (1 + IVA_RATE)


def get_gastos(quote_id: int) -> List[GastoRow]:
    rows = db_fetchall(
        psql("SELECT id, quote_id, categoria, descripcion, monto FROM gastos_trabajo WHERE quote_id=? ORDER BY id"),
        (quote_id,),
    )
    return [GastoRow(
        id=int(r["id"]), quote_id=int(r["quote_id"]),
        categoria=str(r["categoria"]), descripcion=str(r["descripcion"]),
        monto=float(r["monto"]),
    ) for r in rows]


def get_total_gastos(quote_id: int) -> float:
    return sum(g.monto for g in get_gastos(quote_id))


def load_instalaciones_manuales() -> List[InstalacionManualRow]:
    rows = db_fetchall(
        "SELECT id, fecha_instalacion, cliente_nombre, tecnicos, descripcion, monto_cobrado, gastos, notas "
        "FROM instalaciones_manuales ORDER BY fecha_instalacion DESC"
    )
    return [InstalacionManualRow(
        id=int(r["id"]), fecha_instalacion=str(r["fecha_instalacion"]),
        cliente_nombre=str(r["cliente_nombre"]), tecnicos=str(r["tecnicos"]),
        descripcion=str(r["descripcion"]), monto_cobrado=float(r["monto_cobrado"]),
        gastos=float(r["gastos"]), notas=str(r["notas"] or ""),
    ) for r in rows]
