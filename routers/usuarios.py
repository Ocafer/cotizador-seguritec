from __future__ import annotations
import bcrypt

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_roles
from config import EMPRESA_NOMBRE
from database import db_exec, db_fetchone, db_fetchall, db_insert, psql
from templating import render

router = APIRouter()

ROLES_VALIDOS = ["admin", "ventas", "tecnico", "contador"]


@router.get("/usuarios", response_class=HTMLResponse)
def usuarios_get(request: Request):
    gate = require_roles(request, "admin")
    if gate:
        return gate
    rows = db_fetchall("SELECT id, username, rol, activo FROM usuarios ORDER BY username")
    return render(request, "usuarios.html", {
        "empresa": EMPRESA_NOMBRE,
        "usuarios": [dict(r) for r in rows],
        "roles": ROLES_VALIDOS,
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
        "yo": request.session.get("username"),
    })


@router.post("/usuarios/guardar")
def usuarios_guardar(
    request: Request,
    usuario_id: str = Form(""),
    username: str = Form(...),
    password: str = Form(""),
    rol: str = Form(...),
    activo: str = Form("1"),
):
    gate = require_roles(request, "admin")
    if gate:
        return gate

    username = username.strip()
    rol = rol if rol in ROLES_VALIDOS else "ventas"
    activo_val = 1 if activo == "1" else 0

    if usuario_id:
        uid = int(usuario_id)
        if password.strip():
            hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            db_exec(
                psql("UPDATE usuarios SET username=?,password_hash=?,rol=?,activo=? WHERE id=?"),
                (username, hashed, rol, activo_val, uid),
            )
        else:
            db_exec(
                psql("UPDATE usuarios SET username=?,rol=?,activo=? WHERE id=?"),
                (username, rol, activo_val, uid),
            )
        msg = "Usuario+actualizado."
    else:
        if not password.strip():
            return RedirectResponse(url="/usuarios?msg=La+clave+es+requerida.&msg_type=danger", status_code=303)
        existing = db_fetchone(psql("SELECT id FROM usuarios WHERE username=?"), (username,))
        if existing:
            return RedirectResponse(url="/usuarios?msg=Ya+existe+ese+usuario.&msg_type=danger", status_code=303)
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        db_insert(
            "INSERT INTO usuarios(username,password_hash,rol,activo) VALUES(?,?,?,?)",
            (username, hashed, rol, activo_val),
        )
        msg = "Usuario+creado."

    return RedirectResponse(url=f"/usuarios?msg={msg}&msg_type=success", status_code=303)


@router.post("/usuarios/{usuario_id}/borrar")
def usuarios_borrar(request: Request, usuario_id: int):
    gate = require_roles(request, "admin")
    if gate:
        return gate
    yo = request.session.get("username")
    row = db_fetchone(psql("SELECT username FROM usuarios WHERE id=?"), (usuario_id,))
    if row and row["username"] == yo:
        return RedirectResponse(url="/usuarios?msg=No+podés+eliminarte+a+vos+mismo.&msg_type=danger", status_code=303)
    db_exec(psql("DELETE FROM usuarios WHERE id=?"), (usuario_id,))
    return RedirectResponse(url="/usuarios?msg=Usuario+eliminado.&msg_type=warning", status_code=303)
