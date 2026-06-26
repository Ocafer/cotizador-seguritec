from __future__ import annotations
from fastapi import Request
from fastapi.templating import Jinja2Templates
from config import TEMPLATES_DIR, EMPRESA_NOMBRE, EMPRESA_TELF, IVA_RATE

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def render(request: Request, template: str, context: dict | None = None):
    from settings import get_setting
    ctx = {
        "request": request,
        "rol": request.session.get("rol", ""),
        "empresa": get_setting("empresa_nombre", EMPRESA_NOMBRE),
        "telf": get_setting("empresa_telf", EMPRESA_TELF),
        "moneda": get_setting("moneda_simbolo", "Bs"),
        "iva_rate": float(get_setting("iva_rate", str(IVA_RATE))),
    }
    if context:
        ctx.update(context)
    return templates.TemplateResponse(template, ctx)
