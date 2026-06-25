from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_login
from config import EMPRESA_NOMBRE
from database import db_exec, psql
from services import load_all_tecnicos
from templating import templates

router = APIRouter()


@router.get("/tecnicos", response_class=HTMLResponse)
def tecnicos_get(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    return templates.TemplateResponse("tecnicos.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "tecnicos": load_all_tecnicos(),
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
    })


@router.post("/tecnicos/guardar")
def tecnicos_guardar(
    request: Request,
    tecnico_id: str = Form(""),
    nombre: str = Form(...),
    telefono: str = Form(""),
    especialidad: str = Form(""),
    activo: int = Form(1),
):
    gate = require_login(request)
    if gate:
        return gate

    nombre = nombre.strip()
    telefono = telefono.strip() or None
    especialidad = especialidad.strip() or None

    if tecnico_id:
        db_exec(
            psql("UPDATE tecnicos SET nombre=?,telefono=?,especialidad=?,activo=? WHERE id=?"),
            (nombre, telefono, especialidad, activo, int(tecnico_id)),
        )
        msg = "Técnico+actualizado."
    else:
        db_exec(
            psql("INSERT INTO tecnicos(nombre,telefono,especialidad,activo) VALUES(?,?,?,?)"),
            (nombre, telefono, especialidad, activo),
        )
        msg = "Técnico+agregado."

    return RedirectResponse(url=f"/tecnicos?msg={msg}&msg_type=success", status_code=303)


@router.post("/tecnicos/{tecnico_id}/borrar")
def tecnicos_borrar(request: Request, tecnico_id: int):
    gate = require_login(request)
    if gate:
        return gate
    db_exec(psql("DELETE FROM tecnicos WHERE id=?"), (tecnico_id,))
    return RedirectResponse(url="/tecnicos?msg=Técnico+eliminado.&msg_type=warning", status_code=303)
