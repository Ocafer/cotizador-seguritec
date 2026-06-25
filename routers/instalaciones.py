from __future__ import annotations
import json

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_login
from config import EMPRESA_NOMBRE
from database import db_connect, db_exec, db_fetchone, db_fetchall, db_insert, psql
from services import get_quote_total, load_tecnicos_activos, load_instalaciones_manuales, load_all_clientes
from templating import templates

router = APIRouter()


# ---------------------------------------------------------------------------
# Agendar instalaciones
# ---------------------------------------------------------------------------

@router.get("/instalacion/{quote_id}/agendar", response_class=HTMLResponse)
def agendar_get(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    q = db_fetchone(psql("SELECT * FROM quotes WHERE id=?"), (quote_id,))
    inst = db_fetchone(psql("SELECT * FROM instalaciones WHERE quote_id=?"), (quote_id,))

    if not q:
        return RedirectResponse(url="/historial", status_code=303)

    tecnicos_asignados = []
    if inst:
        tecs = db_fetchall(
            psql("SELECT tecnico_id,tecnico_nombre FROM instalacion_tecnicos WHERE instalacion_id=?"),
            (inst["id"],),
        )
        tecnicos_asignados = [{"id": r["tecnico_id"], "nombre": r["tecnico_nombre"]} for r in tecs]

    return templates.TemplateResponse("agendar.html", {
        "request": request, "q": q, "instalacion": inst,
        "empresa": EMPRESA_NOMBRE,
        "tecnicos_activos": load_tecnicos_activos(),
        "tecnicos_asignados_json": json.dumps(tecnicos_asignados),
    })


@router.post("/instalacion/{quote_id}/guardar")
def agendar_post(
    request: Request,
    quote_id: int,
    fecha_instalacion: str = Form(...),
    tecnicos_json: str = Form("[]"),
    estado: str = Form("pendiente"),
    notas_instalacion: str = Form(""),
):
    gate = require_login(request)
    if gate:
        return gate

    try:
        tecs = json.loads(tecnicos_json)
    except Exception:
        tecs = []

    tecnico_str = ", ".join(t["nombre"] for t in tecs) if tecs else "Sin asignar"
    notas = notas_instalacion.strip() or None

    existing = db_fetchone(psql("SELECT id FROM instalaciones WHERE quote_id=?"), (quote_id,))
    if existing:
        inst_id = int(existing["id"])
        db_exec(
            psql("UPDATE instalaciones SET fecha_instalacion=?,tecnico=?,estado=?,notas_instalacion=? WHERE id=?"),
            (fecha_instalacion, tecnico_str, estado, notas, inst_id),
        )
        db_exec(psql("DELETE FROM instalacion_tecnicos WHERE instalacion_id=?"), (inst_id,))
    else:
        inst_id = db_insert(
            "INSERT INTO instalaciones(quote_id,fecha_instalacion,tecnico,estado,notas_instalacion) VALUES(?,?,?,?,?)",
            (quote_id, fecha_instalacion, tecnico_str, estado, notas),
        )

    for t in tecs:
        db_exec(
            psql("INSERT INTO instalacion_tecnicos(instalacion_id,tecnico_id,tecnico_nombre) VALUES(?,?,?)"),
            (inst_id, t.get("id"), t["nombre"]),
        )

    return RedirectResponse(url="/agenda", status_code=303)


# ---------------------------------------------------------------------------
# Agenda
# ---------------------------------------------------------------------------

@router.get("/agenda", response_class=HTMLResponse)
def agenda(request: Request, fecha: str = ""):
    from datetime import datetime
    gate = require_login(request)
    if gate:
        return gate

    fecha_sel = fecha or datetime.now().strftime("%Y-%m-%d")

    rows_dia = db_fetchall(psql("""
        SELECT i.quote_id,i.fecha_instalacion,i.tecnico,i.estado,i.notas_instalacion,
               q.quote_no,q.client_name
        FROM instalaciones i JOIN quotes q ON q.id = i.quote_id
        WHERE i.fecha_instalacion = ? ORDER BY i.tecnico
    """), (fecha_sel,))
    rows_proximas = db_fetchall(psql("""
        SELECT i.quote_id,i.fecha_instalacion,i.tecnico,i.estado,q.quote_no,q.client_name
        FROM instalaciones i JOIN quotes q ON q.id = i.quote_id
        WHERE i.fecha_instalacion > ? AND i.estado != 'completada'
        ORDER BY i.fecha_instalacion LIMIT 20
    """), (fecha_sel,))

    def enrich(rows):
        return [{
            "quote_id": r["quote_id"], "quote_no": r["quote_no"],
            "client_name": r["client_name"],
            "fecha_instalacion": str(r["fecha_instalacion"]),
            "tecnico": r["tecnico"], "estado": r["estado"],
            "notas_instalacion": r.get("notas_instalacion", ""),
            "total_con_iva": get_quote_total(int(r["quote_id"])),
        } for r in rows]

    instalaciones_dia = enrich(rows_dia)
    proximas = enrich(rows_proximas)

    return templates.TemplateResponse("agenda.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "fecha_sel": fecha_sel,
        "instalaciones_dia": instalaciones_dia,
        "proximas": proximas,
        "total_dia": len(instalaciones_dia),
    })


# ---------------------------------------------------------------------------
# Instalaciones manuales (historial sin cotización)
# ---------------------------------------------------------------------------

@router.get("/instalaciones-pasadas", response_class=HTMLResponse)
def instalaciones_pasadas_get(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    registros = load_instalaciones_manuales()
    total_cobrado = sum(r.monto_cobrado for r in registros)
    total_gastos = sum(r.gastos for r in registros)
    productos = db_fetchall(
        "SELECT sku,nombre,unidad,precio_bs FROM products WHERE activo=1 ORDER BY categoria,nombre"
    )
    return templates.TemplateResponse("instalaciones_pasadas.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "registros": registros,
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
        "total_cobrado": total_cobrado,
        "total_gastos": total_gastos,
        "utilidad_total": total_cobrado - total_gastos,
        "tecnicos_activos": load_tecnicos_activos(),
        "clientes": load_all_clientes(),
        "productos": [dict(p) for p in productos],
    })


@router.post("/instalaciones-pasadas/guardar")
def instalaciones_pasadas_guardar(
    request: Request,
    registro_id: int = Form(0),
    fecha_instalacion: str = Form(...),
    cliente_nombre: str = Form(...),
    tecnicos: str = Form(...),
    descripcion: str = Form(...),
    monto_cobrado: float = Form(0),
    gastos: float = Form(0),
    notas: str = Form(""),
):
    gate = require_login(request)
    if gate:
        return gate

    if registro_id:
        db_exec(
            psql("UPDATE instalaciones_manuales SET fecha_instalacion=?,cliente_nombre=?,tecnicos=?,descripcion=?,monto_cobrado=?,gastos=?,notas=? WHERE id=?"),
            (fecha_instalacion, cliente_nombre, tecnicos, descripcion, monto_cobrado, gastos, notas, registro_id),
        )
        msg = "Registro+actualizado."
    else:
        db_exec(
            psql("INSERT INTO instalaciones_manuales(fecha_instalacion,cliente_nombre,tecnicos,descripcion,monto_cobrado,gastos,notas) VALUES(?,?,?,?,?,?,?)"),
            (fecha_instalacion, cliente_nombre, tecnicos, descripcion, monto_cobrado, gastos, notas),
        )
        msg = "Registro+guardado."

    return RedirectResponse(url=f"/instalaciones-pasadas?msg={msg}&msg_type=success", status_code=303)


@router.post("/instalaciones-pasadas/{registro_id}/borrar")
def instalaciones_pasadas_borrar(request: Request, registro_id: int):
    gate = require_login(request)
    if gate:
        return gate
    db_exec(psql("DELETE FROM instalaciones_manuales WHERE id=?"), (registro_id,))
    return RedirectResponse(url="/instalaciones-pasadas?msg=Registro+eliminado.&msg_type=warning", status_code=303)
