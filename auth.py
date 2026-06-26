from __future__ import annotations
import bcrypt
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from database import db_fetchone, psql
from templating import render

router = APIRouter()

_REDIRECT_BY_ROL = {
    "admin": "/dashboard",
    "ventas": "/dashboard",
    "contador": "/dashboard",
    "tecnico": "/agenda",
}


def is_logged_in(request: Request) -> bool:
    return bool(request.session.get("username"))


def get_rol(request: Request) -> str:
    return request.session.get("rol", "")


def require_roles(request: Request, *roles: str):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    if roles and get_rol(request) not in roles:
        return RedirectResponse(url="/sin-permiso", status_code=303)
    return None


@router.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    if is_logged_in(request):
        return RedirectResponse(url=_REDIRECT_BY_ROL.get(get_rol(request), "/dashboard"), status_code=303)
    return render(request, "login.html", {"err": request.query_params.get("err")})


@router.post("/login")
def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    row = db_fetchone(
        psql("SELECT username, password_hash, rol, activo FROM usuarios WHERE username=?"),
        (username.strip(),),
    )
    if row and int(row["activo"]) and bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        request.session["username"] = row["username"]
        request.session["rol"] = row["rol"]
        return RedirectResponse(url=_REDIRECT_BY_ROL.get(row["rol"], "/dashboard"), status_code=303)
    return RedirectResponse(url="/login?err=Usuario+o+clave+incorrecta", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
