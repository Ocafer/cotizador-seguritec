from __future__ import annotations
import os
import secrets
from dotenv import load_dotenv

load_dotenv()

APP_TITLE = "Cotizador - Seguritec Tarija"
EMPRESA_NOMBRE = "Seguritec Tarija"
EMPRESA_TELF = "70218010"
IVA_RATE = 0.13

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")
DB_PATH = os.path.join(BASE_DIR, "app.db")
EXCEL_PATH = os.path.join(DATA_DIR, "precios.xlsx")

DATABASE_URL = os.environ.get("DATABASE_URL")

ADMIN_USER = os.environ.get("ADMIN_USER", "seguritec")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "cambia_esto")

SESSION_SECRET = os.environ.get("SESSION_SECRET", secrets.token_urlsafe(32))
LOGO_PATH = os.environ.get("LOGO_PATH", os.path.join(STATIC_DIR, "logo.png"))
