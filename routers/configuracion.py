from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_roles
from database import db_fetchall
from settings import get_setting, set_setting
from templating import render

router = APIRouter()

_KEYS = ["empresa_nombre", "empresa_telf", "empresa_email", "empresa_direccion", "moneda_simbolo", "iva_rate"]
_ROLES = ["admin", "ventas", "tecnico", "contador"]


@router.get("/configuracion", response_class=HTMLResponse)
def configuracion_get(request: Request):
    gate = require_roles(request, "admin")
    if gate:
        return gate
    cfg = {k: get_setting(k) for k in _KEYS}
    usuarios = db_fetchall("SELECT id, username, rol, activo FROM usuarios ORDER BY username")
    return render(request, "configuracion.html", {
        "cfg": cfg,
        "usuarios": [dict(u) for u in usuarios],
        "roles": _ROLES,
        "yo": request.session.get("username", ""),
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
        "tab": request.query_params.get("tab", "empresa"),
    })


@router.post("/configuracion/guardar")
def configuracion_guardar(
    request: Request,
    empresa_nombre: str = Form(...),
    empresa_telf: str = Form(""),
    empresa_email: str = Form(""),
    empresa_direccion: str = Form(""),
    moneda_simbolo: str = Form("Bs"),
    iva_rate: str = Form("0.13"),
):
    gate = require_roles(request, "admin")
    if gate:
        return gate

    try:
        iva = float(iva_rate.replace(",", "."))
        if not (0 <= iva <= 1):
            raise ValueError
    except ValueError:
        return RedirectResponse(
            url="/configuracion?msg=IVA+debe+ser+un+número+entre+0+y+1+(ej.+0.13)&msg_type=danger",
            status_code=303,
        )

    set_setting("empresa_nombre", empresa_nombre.strip())
    set_setting("empresa_telf", empresa_telf.strip())
    set_setting("empresa_email", empresa_email.strip())
    set_setting("empresa_direccion", empresa_direccion.strip())
    set_setting("moneda_simbolo", moneda_simbolo.strip() or "Bs")
    set_setting("iva_rate", str(iva))
    return RedirectResponse(url="/configuracion?msg=Configuración+guardada.&msg_type=success", status_code=303)
