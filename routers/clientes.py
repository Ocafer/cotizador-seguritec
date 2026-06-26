from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_roles
from config import EMPRESA_NOMBRE
from database import db_exec, psql
from services import load_all_clientes
from templating import render

router = APIRouter()


@router.get("/clientes", response_class=HTMLResponse)
def clientes_get(request: Request):
    gate = require_roles(request, "admin", "ventas")
    if gate:
        return gate
    return render(request, "clientes.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "clientes": load_all_clientes(),
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
    })


@router.post("/clientes/guardar")
def clientes_guardar(
    request: Request,
    cliente_id: int = Form(0),
    nombre: str = Form(...),
    telefono: str = Form(""),
    direccion: str = Form(""),
    ci_nit: str = Form(""),
    tipo: str = Form("residencial"),
    notas: str = Form(""),
):
    gate = require_roles(request, "admin", "ventas")
    if gate:
        return gate

    if cliente_id:
        db_exec(
            psql("UPDATE clientes SET nombre=?,telefono=?,direccion=?,ci_nit=?,tipo=?,notas=? WHERE id=?"),
            (nombre, telefono, direccion, ci_nit, tipo, notas, cliente_id),
        )
        msg = "Cliente+actualizado."
    else:
        db_exec(
            psql("INSERT INTO clientes(nombre,telefono,direccion,ci_nit,tipo,notas) VALUES(?,?,?,?,?,?)"),
            (nombre, telefono, direccion, ci_nit, tipo, notas),
        )
        msg = "Cliente+agregado."

    return RedirectResponse(url=f"/clientes?msg={msg}&msg_type=success", status_code=303)


@router.post("/clientes/{cliente_id}/borrar")
def clientes_borrar(request: Request, cliente_id: int):
    gate = require_roles(request, "admin", "ventas")
    if gate:
        return gate
    db_exec(psql("DELETE FROM clientes WHERE id=?"), (cliente_id,))
    return RedirectResponse(url="/clientes?msg=Cliente+eliminado.&msg_type=warning", status_code=303)
