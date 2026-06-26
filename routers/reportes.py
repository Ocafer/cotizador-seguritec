from __future__ import annotations
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from auth import require_roles
from database import db_fetchall, psql
from settings import get_setting
from templating import render

router = APIRouter()


@router.get("/reportes", response_class=HTMLResponse)
def reportes(request: Request, desde: str = "", hasta: str = "", tecnico: str = ""):
    gate = require_roles(request, "admin", "ventas", "contador")
    if gate:
        return gate

    iva_rate = float(get_setting("iva_rate", "0.13"))

    hoy = datetime.now().strftime("%Y-%m-%d")
    desde = desde or datetime.now().strftime("%Y-%m-01")
    hasta = hasta or hoy

    rows = db_fetchall(psql("""
        SELECT i.quote_id,i.fecha_instalacion,i.tecnico,i.estado,q.quote_no,q.client_name
        FROM instalaciones i JOIN quotes q ON q.id = i.quote_id
        WHERE i.fecha_instalacion BETWEEN ? AND ? ORDER BY i.fecha_instalacion
    """), (desde, hasta))
    tecnicos_rows = db_fetchall("SELECT DISTINCT tecnico FROM instalaciones ORDER BY tecnico")
    tecnicos = [r["tecnico"] for r in tecnicos_rows]

    filtered = [r for r in rows if not tecnico or r["tecnico"] == tecnico]

    # Bulk-fetch totals and gastos (2 queries instead of 2×N)
    totales: dict = {}
    gastos_map: dict = {}
    if filtered:
        ids = list({int(r["quote_id"]) for r in filtered})
        ph = ",".join("?" * len(ids))
        for row in db_fetchall(
            psql(f"SELECT quote_id, COALESCE(SUM(qty*unit_price), 0) AS subtotal FROM quote_items WHERE quote_id IN ({ph}) GROUP BY quote_id"),
            tuple(ids),
        ):
            totales[int(row["quote_id"])] = float(row["subtotal"] or 0) * (1 + iva_rate)
        for row in db_fetchall(
            psql(f"SELECT quote_id, COALESCE(SUM(monto), 0) AS total FROM gastos_trabajo WHERE quote_id IN ({ph}) GROUP BY quote_id"),
            tuple(ids),
        ):
            gastos_map[int(row["quote_id"])] = float(row["total"] or 0)

    instalaciones = []
    for r in filtered:
        total = totales.get(int(r["quote_id"]), 0.0)
        total_gastos = gastos_map.get(int(r["quote_id"]), 0.0)
        utilidad = total - total_gastos
        margen = (utilidad / total * 100) if total > 0 else 0
        instalaciones.append({
            "quote_id": r["quote_id"], "quote_no": r["quote_no"],
            "client_name": r["client_name"],
            "fecha_instalacion": str(r["fecha_instalacion"]),
            "tecnico": r["tecnico"], "estado": r["estado"],
            "total_con_iva": total, "total_gastos": total_gastos,
            "utilidad": utilidad, "margen": margen,
        })

    total_con_iva = sum(i["total_con_iva"] for i in instalaciones)
    total_sin_iva = total_con_iva / (1 + iva_rate)
    total_iva = total_con_iva - total_sin_iva
    total_gastos_global = sum(i["total_gastos"] for i in instalaciones)
    utilidad_global = total_con_iva - total_gastos_global
    margen_global = (utilidad_global / total_con_iva * 100) if total_con_iva > 0 else 0

    return render(request, "reportes.html", {
        "desde": desde, "hasta": hasta, "tecnico_filtro": tecnico,
        "tecnicos": tecnicos, "instalaciones": instalaciones,
        "stats": {
            "total": len(instalaciones),
            "pendientes": sum(1 for i in instalaciones if i["estado"] == "pendiente"),
            "en_curso": sum(1 for i in instalaciones if i["estado"] == "en_curso"),
            "completadas": sum(1 for i in instalaciones if i["estado"] == "completada"),
            "total_sin_iva": total_sin_iva, "total_iva": total_iva,
            "total_con_iva": total_con_iva, "total_gastos": total_gastos_global,
            "utilidad": utilidad_global, "margen": margen_global,
        },
    })
