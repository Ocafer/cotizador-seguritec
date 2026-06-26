from __future__ import annotations
import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles

from config import APP_TITLE, SESSION_SECRET, STATIC_DIR
from schema import seed_products_from_excel_if_empty
from templating import templates

from auth import router as auth_router
from routers.cotizaciones import router as cot_router
from routers.productos import router as prod_router
from routers.instalaciones import router as inst_router
from routers.tecnicos import router as tec_router
from routers.gastos import router as gastos_router
from routers.reportes import router as rep_router
from routers.clientes import router as cli_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI(title=APP_TITLE)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)
    logger.error("HTTP %s en %s %s", exc.status_code, request.method, request.url.path)
    return templates.TemplateResponse("500.html", {"request": request}, status_code=exc.status_code)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Error no controlado en %s %s", request.method, request.url.path)
    return templates.TemplateResponse("500.html", {"request": request}, status_code=500)

if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.include_router(auth_router)
app.include_router(cot_router)
app.include_router(prod_router)
app.include_router(inst_router)
app.include_router(tec_router)
app.include_router(gastos_router)
app.include_router(rep_router)
app.include_router(cli_router)

# Run pending migrations at startup (creates tables on first deploy, applies changes on updates)
from alembic.config import Config as AlembicConfig
from alembic import command as alembic_command
_alembic_cfg = AlembicConfig(os.path.join(os.path.dirname(__file__), "alembic.ini"))
alembic_command.upgrade(_alembic_cfg, "head")

seed_products_from_excel_if_empty()
