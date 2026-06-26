from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_roles
from config import EMPRESA_NOMBRE
from database import db_exec, db_fetchone, psql
from services import get_gastos, get_quote_total
from templating import render

router = APIRouter()


@router.get("/gastos/{quote_id}", response_class=HTMLResponse)
def gastos_get(request: Request, quote_id: int):
    gate = require_roles(request, "admin", "ventas", "tecnico", "contador")
    if gate:
        return gate

    q = db_fetchone(psql("SELECT id,quote_no,client_name,created_at FROM quotes WHERE id=?"), (quote_id,))
    if not q:
        return RedirectResponse(url="/historial", status_code=303)

    gastos = get_gastos(quote_id)
    total_cotizacion = get_quote_total(quote_id)
    total_gastos = sum(g.monto for g in gastos)
    utilidad = total_cotizacion - total_gastos
    margen = (utilidad / total_cotizacion * 100) if total_cotizacion > 0 else 0

    desglose: dict = {}
    for g in gastos:
        desglose[g.categoria] = desglose.get(g.categoria, 0) + g.monto

    return render(request, "gastos.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "q": dict(q), "gastos": gastos,
        "total_cotizacion": total_cotizacion,
        "total_gastos": total_gastos,
        "utilidad": utilidad,
        "margen": margen,
        "desglose": desglose,
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
    })


@router.post("/gastos/{quote_id}/agregar")
def gastos_agregar(
    request: Request,
    quote_id: int,
    categoria: str = Form(...),
    descripcion: str = Form(...),
    monto: float = Form(...),
):
    gate = require_roles(request, "admin", "ventas", "tecnico", "contador")
    if gate:
        return gate
    db_exec(
        psql("INSERT INTO gastos_trabajo(quote_id,categoria,descripcion,monto) VALUES(?,?,?,?)"),
        (quote_id, categoria, descripcion, monto),
    )
    return RedirectResponse(url=f"/gastos/{quote_id}?msg=Gasto+agregado.&msg_type=success", status_code=303)


@router.post("/gastos/{gasto_id}/borrar")
def gastos_borrar(request: Request, gasto_id: int):
    gate = require_roles(request, "admin", "ventas", "tecnico", "contador")
    if gate:
        return gate
    g = db_fetchone(psql("SELECT quote_id FROM gastos_trabajo WHERE id=?"), (gasto_id,))
    db_exec(psql("DELETE FROM gastos_trabajo WHERE id=?"), (gasto_id,))
    quote_id = int(g["quote_id"]) if g else 0
    return RedirectResponse(url=f"/gastos/{quote_id}?msg=Gasto+eliminado.&msg_type=warning", status_code=303)
