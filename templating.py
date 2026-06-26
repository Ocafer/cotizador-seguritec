from __future__ import annotations
from fastapi import Request
from fastapi.templating import Jinja2Templates
from config import TEMPLATES_DIR

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def render(request: Request, template: str, context: dict | None = None):
    ctx = {"request": request, "rol": request.session.get("rol", "")}
    if context:
        ctx.update(context)
    return templates.TemplateResponse(template, ctx)
