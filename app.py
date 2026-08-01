from __future__ import annotations
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles

from config import APP_TITLE, SESSION_SECRET, STATIC_DIR
from schema import seed_products_from_excel_if_empty, seed_admin_user, seed_configuracion
from templating import templates

from auth import router as auth_router
from routers.cotizaciones import router as cot_router
from routers.productos import router as prod_router
from routers.instalaciones import router as inst_router
from routers.tecnicos import router as tec_router
from routers.gastos import router as gastos_router
from routers.reportes import router as rep_router
from routers.clientes import router as cli_router
from routers.usuarios import router as usr_router
from routers.configuracion import router as cfg_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Seeds ya corrieron en el primer deploy — Railway debe tener SEEDS_DONE=true
_SEEDS_DONE = os.environ.get("SEEDS_DONE", "false").lower() == "true"


@asynccontextmanager
async def lifespan(app: FastAPI):
    from alembic.config import Config as AlembicConfig
    from alembic import command as alembic_command

    try:
        cfg = AlembicConfig(os.path.join(os.path.dirname(__file__), "alembic.ini"))
        alembic_command.upgrade(cfg, "head")
        logger.info("Migraciones aplicadas.")
    except Exception:
        logger.exception("Error aplicando migraciones — la app continúa.")

    if not _SEEDS_DONE:
        for fn in (seed_admin_user, seed_configuracion, seed_products_from_excel_if_empty):
            try:
                fn()
            except Exception:
                logger.exception("Error en seed %s — se omite.", fn.__name__)
        logger.info("Seeds completados. Setea SEEDS_DONE=true en Railway para omitirlos en futuros reinicios.")
    else:
        logger.info("SEEDS_DONE=true — seeds omitidos.")

    yield


app = FastAPI(title=APP_TITLE, lifespan=lifespan)
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
app.include_router(usr_router)
app.include_router(cfg_router)
