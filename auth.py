from __future__ import annotations
import bcrypt
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from config import ADMIN_USER, ADMIN_PASS_HASH
from templating import templates

router = APIRouter()


def is_logged_in(request: Request) -> bool:
    return bool(request.session.get("auth"))


def require_login(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


@router.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    if is_logged_in(request):
        return RedirectResponse(url="/nueva", status_code=303)
    return templates.TemplateResponse("login.html", {
        "request": request,
        "err": request.query_params.get("err"),
    })


@router.post("/login")
def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and bcrypt.checkpw(password.encode(), ADMIN_PASS_HASH):
        request.session["auth"] = True
        return RedirectResponse(url="/nueva", status_code=303)
    return RedirectResponse(url="/login?err=Usuario+o+clave+incorrecta", status_code=303)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
