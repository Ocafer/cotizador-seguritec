from __future__ import annotations

import io
import os
import sqlite3
import secrets
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Any, Dict, Tuple

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import load_workbook
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles

# PostgreSQL (solo se usa si existe DATABASE_URL)
import psycopg2
import psycopg2.extras


# =========================
# Config
# =========================
APP_TITLE = "Cotizador - Seguritec Tarija"
EMPRESA_NOMBRE = "Seguritec Tarija"
EMPRESA_TELF = "70218010"
IVA_RATE = 0.13  # 13%

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

DB_PATH = os.path.join(BASE_DIR, "app.db")               # SQLite local
EXCEL_PATH = os.path.join(DATA_DIR, "precios.xlsx")

DATABASE_URL = os.environ.get("DATABASE_URL")            # PostgreSQL en Render

ADMIN_USER = os.environ.get("ADMIN_USER", "seguritec")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "cambia_esto")
SESSION_SECRET = os.environ.get("SESSION_SECRET", secrets.token_urlsafe(32))

app = FastAPI(title=APP_TITLE)
templates = Jinja2Templates(directory=TEMPLATES_DIR)

app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# =========================
# Auth helpers
# =========================
def is_logged_in(request: Request) -> bool:
    return bool(request.session.get("auth"))


def require_login(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


# =========================
# DB helpers (SQLite o PostgreSQL)
# =========================
def _using_postgres() -> bool:
    return bool(DATABASE_URL)


def db_connect():
    """
    Devuelve (con, engine) donde engine es 'postgres' o 'sqlite'
    """
    if _using_postgres():
        # Render internal DB URL normalmente funciona sin SSL, pero "require" también suele funcionar.
        # Si algún día falla por SSL, cambia PGSSLMODE a 'disable' en Render.
        sslmode = os.environ.get("PGSSLMODE", "require")
        con = psycopg2.connect(DATABASE_URL)
        return con, "postgres"
    else:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        return con, "sqlite"


def db_execute(sql: str, params: Tuple[Any, ...] = ()) -> None:
    con, engine = db_connect()
    cur = con.cursor()
    cur.execute(sql, params)
    con.commit()
    con.close()


def db_fetchone(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    con, engine = db_connect()
    if engine == "postgres":
        cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        row = cur.fetchone()
        con.close()
        return dict(row) if row else None
    else:
        cur = con.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        con.close()
        return dict(row) if row else None


def db_fetchall(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    con, engine = db_connect()
    if engine == "postgres":
        cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        rows = cur.fetchall()
        con.close()
        return [dict(r) for r in rows]
    else:
        cur = con.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        con.close()
        return [dict(r) for r in rows]


def init_db() -> None:
    con, engine = db_connect()
    cur = con.cursor()

    if engine == "postgres":
        # Tipos PostgreSQL
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                id SERIAL PRIMARY KEY,
                quote_no INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                client_name TEXT NOT NULL,
                delivery_time TEXT NOT NULL,
                validity_days INTEGER NOT NULL,
                notes TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quote_items (
                id SERIAL PRIMARY KEY,
                quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
                sku TEXT,
                name TEXT NOT NULL,
                unit TEXT NOT NULL,
                qty DOUBLE PRECISION NOT NULL,
                unit_price DOUBLE PRECISION NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS counter (
                counter_key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        cur.execute("""
            INSERT INTO counter(counter_key, value)
            VALUES ('quote_no', 0)
            ON CONFLICT (counter_key) DO NOTHING
        """)
        con.commit()
        con.close()

    else:
        # Tipos SQLite
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_no INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                client_name TEXT NOT NULL,
                delivery_time TEXT NOT NULL,
                validity_days INTEGER NOT NULL,
                notes TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS quote_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id INTEGER NOT NULL,
                sku TEXT,
                name TEXT NOT NULL,
                unit TEXT NOT NULL,
                qty REAL NOT NULL,
                unit_price REAL NOT NULL,
                FOREIGN KEY (quote_id) REFERENCES quotes(id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS counter (
                counter_key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        cur.execute("INSERT OR IGNORE INTO counter(counter_key, value) VALUES('quote_no', 0)")
        con.commit()
        con.close()


def next_quote_no() -> int:
    con, engine = db_connect()
    cur = con.cursor()

    if engine == "postgres":
        cur.execute("""
            UPDATE counter
            SET value = value + 1
            WHERE counter_key = 'quote_no'
            RETURNING value
        """)
        n = cur.fetchone()[0]
        con.commit()
        con.close()
        return int(n)
    else:
        cur.execute("UPDATE counter SET value = value + 1 WHERE counter_key='quote_no'")
        cur.execute("SELECT value FROM counter WHERE counter_key='quote_no'")
        n = cur.fetchone()[0]
        con.commit()
        con.close()
        return int(n)


# Inicializa tablas al arrancar
init_db()


# =========================
# Catalog from Excel (openpyxl)
# =========================
@dataclass
class Product:
    sku: str
    categoria: str
    nombre: str
    unidad: str
    precio_bs: float


def _norm_header(x) -> str:
    return str(x).strip().lower() if x is not None else ""


def _to_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        s = str(x).strip().replace(",", ".")
        try:
            return float(s)
        except Exception:
            return default


def _to_int(x, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def load_products() -> List[Product]:
    if not os.path.exists(EXCEL_PATH):
        return []

    wb = load_workbook(EXCEL_PATH, data_only=True)
    if "productos" not in wb.sheetnames:
        raise ValueError("El Excel debe tener una hoja llamada 'productos'.")

    ws = wb["productos"]

    headers = {}
    for col_idx, cell in enumerate(ws[1], start=1):
        h = _norm_header(cell.value)
        if h:
            headers[h] = col_idx

    required = {"sku", "categoria", "nombre", "unidad", "precio_bs", "activo"}
    missing = required - set(headers.keys())
    if missing:
        raise ValueError(f"Faltan columnas en Excel: {', '.join(sorted(missing))}")

    products: List[Product] = []
    for r in range(2, ws.max_row + 1):
        activo = _to_int(ws.cell(r, headers["activo"]).value, default=0)
        if activo != 1:
            continue

        sku = str(ws.cell(r, headers["sku"]).value or "").strip()
        categoria = str(ws.cell(r, headers["categoria"]).value or "").strip()
        nombre = str(ws.cell(r, headers["nombre"]).value or "").strip()
        unidad = str(ws.cell(r, headers["unidad"]).value or "").strip() or "unidad"
        precio_bs = _to_float(ws.cell(r, headers["precio_bs"]).value, default=0.0)

        if not nombre and not sku:
            continue

        products.append(Product(sku=sku, categoria=categoria, nombre=nombre, unidad=unidad, precio_bs=precio_bs))

    products.sort(key=lambda p: (p.categoria.lower(), p.nombre.lower()))
    return products


# =========================
# PDF generator
# =========================
def money(x: float) -> str:
    return f"Bs {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def generate_pdf(
    quote_no: int,
    created_at: str,
    client_name: str,
    delivery_time: str,
    validity_days: int,
    items: List[dict],
    notes: Optional[str] = None,
) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    x0 = 18 * mm
    y = height - 18 * mm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x0, y, EMPRESA_NOMBRE)
    c.setFont("Helvetica", 10)
    y -= 6 * mm
    c.drawString(x0, y, f"Telf.: {EMPRESA_TELF}")
    y -= 10 * mm

    c.setFont("Helvetica-Bold", 13)
    c.drawString(x0, y, "COTIZACIÓN")
    y -= 8 * mm

    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"N°: {quote_no:06d}")
    c.drawRightString(width - x0, y, f"Fecha: {created_at}")
    y -= 6 * mm
    c.drawString(x0, y, f"Cliente: {client_name}")
    y -= 6 * mm
    c.drawString(x0, y, f"Tiempo de entrega: {delivery_time}")
    y -= 6 * mm
    c.drawString(x0, y, f"Validez de la propuesta: {validity_days} día(s)")
    y -= 10 * mm

    c.setFont("Helvetica-Bold", 10)
    c.drawString(x0, y, "Ítem")
    c.drawString(x0 + 90 * mm, y, "Cant.")
    c.drawString(x0 + 110 * mm, y, "P. Unit.")
    c.drawString(x0 + 145 * mm, y, "Importe")
    y -= 4 * mm
    c.line(x0, y, width - x0, y)
    y -= 6 * mm

    c.setFont("Helvetica", 10)
    subtotal = 0.0

    for it in items:
        name = str(it["name"])
        qty = float(it["qty"])
        unit = str(it["unit"])
        unit_price = float(it["unit_price"])
        line_total = qty * unit_price
        subtotal += line_total

        shown = name if len(name) <= 52 else name[:49] + "..."
        c.drawString(x0, y, f"{shown} ({unit})")
        c.drawRightString(x0 + 105 * mm, y, f"{qty:g}")
        c.drawRightString(x0 + 140 * mm, y, money(unit_price))
        c.drawRightString(width - x0, y, money(line_total))
        y -= 6 * mm

        if y < 30 * mm:
            c.showPage()
            y = height - 18 * mm
            c.setFont("Helvetica", 10)

    y -= 2 * mm
    c.line(x0, y, width - x0, y)
    y -= 8 * mm

    iva = subtotal * IVA_RATE
    total = subtotal + iva

    c.setFont("Helvetica-Bold", 10)
    c.drawRightString(x0 + 140 * mm, y, "Subtotal (Sin IVA):")
    c.drawRightString(width - x0, y, money(subtotal))
    y -= 6 * mm

    c.setFont("Helvetica-Bold", 10)
    c.drawRightString(x0 + 140 * mm, y, f"IVA ({int(IVA_RATE * 100)}%):")
    c.drawRightString(width - x0, y, money(iva))
    y -= 6 * mm

    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(x0 + 140 * mm, y, "Total (Con IVA):")
    c.drawRightString(width - x0, y, money(total))
    y -= 10 * mm

    if notes:
        c.setFont("Helvetica-Bold", 10)
        c.drawString(x0, y, "Notas:")
        y -= 6 * mm
        c.setFont("Helvetica", 10)
        for line in str(notes).splitlines():
            c.drawString(x0, y, line[:95])
            y -= 5 * mm

    c.setFont("Helvetica", 8)
    c.drawString(x0, 12 * mm, f"{EMPRESA_NOMBRE} - Cotización generada automáticamente")
    c.showPage()
    c.save()
    return buf.getvalue()


# =========================
# Auth routes
# =========================
@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    if is_logged_in(request):
        return RedirectResponse(url="/nueva", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "err": request.query_params.get("err")})


@app.post("/login")
def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    # strip para evitar espacios al copiar/pegar
    if username.strip() == ADMIN_USER.strip() and password.strip() == ADMIN_PASS.strip():
        request.session["auth"] = True
        return RedirectResponse(url="/nueva", status_code=303)
    return RedirectResponse(url="/login?err=Usuario+o+clave+incorrecta", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


# =========================
# Routes
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return RedirectResponse(url="/nueva", status_code=303)


@app.get("/nueva", response_class=HTMLResponse)
def nueva(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    products = load_products()
    return templates.TemplateResponse(
        "nueva.html",
        {"request": request, "products": products, "empresa": EMPRESA_NOMBRE, "telf": EMPRESA_TELF, "iva_rate": IVA_RATE},
    )


@app.post("/crear")
def crear_cotizacion(
    request: Request,
    client_name: str = Form(...),
    delivery_time: str = Form(...),
    validity_days: int = Form(...),
    notes: str = Form(""),
    item_sku: List[str] = Form([]),
    item_name: List[str] = Form([]),
    item_unit: List[str] = Form([]),
    item_qty: List[float] = Form([]),
    item_unit_price: List[float] = Form([]),
):
    gate = require_login(request)
    if gate:
        return gate

    items = []
    for i in range(len(item_name)):
        name = (item_name[i] or "").strip()
        if not name:
            continue
        qty = float(item_qty[i] or 0)
        price = float(item_unit_price[i] or 0)
        if qty <= 0:
            continue
        items.append({
            "sku": (item_sku[i] or "").strip(),
            "name": name,
            "unit": (item_unit[i] or "unidad").strip(),
            "qty": qty,
            "unit_price": price,
        })

    if not items:
        return RedirectResponse(url="/nueva?err=Agrega+al+menos+un+item", status_code=303)

    qno = next_quote_no()
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    con, engine = db_connect()
    cur = con.cursor()

    if engine == "postgres":
        cur.execute(
            "INSERT INTO quotes(quote_no, created_at, client_name, delivery_time, validity_days, notes) "
            "VALUES(%s,%s,%s,%s,%s,%s) RETURNING id",
            (qno, created_at, client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None),
        )
        quote_id = cur.fetchone()[0]

        for it in items:
            cur.execute(
                "INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) "
                "VALUES(%s,%s,%s,%s,%s,%s)",
                (quote_id, it["sku"], it["name"], it["unit"], it["qty"], it["unit_price"]),
            )

    else:
        cur.execute(
            "INSERT INTO quotes(quote_no, created_at, client_name, delivery_time, validity_days, notes) "
            "VALUES(?,?,?,?,?,?)",
            (qno, created_at, client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None),
        )
        quote_id = cur.lastrowid

        for it in items:
            cur.execute(
                "INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) "
                "VALUES(?,?,?,?,?,?)",
                (quote_id, it["sku"], it["name"], it["unit"], it["qty"], it["unit_price"]),
            )

    con.commit()
    con.close()

    return RedirectResponse(url=f"/cotizacion/{quote_id}/pdf", status_code=303)


@app.get("/historial", response_class=HTMLResponse)
def historial(request: Request):
    gate = require_login(request)
    if gate:
        return gate

    quotes = db_fetchall("SELECT * FROM quotes ORDER BY id DESC LIMIT 500")
    return templates.TemplateResponse("historial.html", {"request": request, "quotes": quotes, "empresa": EMPRESA_NOMBRE})


@app.get("/cotizacion/{quote_id}/pdf")
def cotizacion_pdf(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    engine = "postgres" if _using_postgres() else "sqlite"
    placeholder = "%s" if engine == "postgres" else "?"

    q = db_fetchone(f"SELECT * FROM quotes WHERE id={placeholder}", (quote_id,))
    if not q:
        return RedirectResponse(url="/historial", status_code=303)

    items = db_fetchall(f"SELECT * FROM quote_items WHERE quote_id={placeholder} ORDER BY id", (quote_id,))

    pdf = generate_pdf(
        quote_no=int(q["quote_no"]),
        created_at=str(q["created_at"]),
        client_name=str(q["client_name"]),
        delivery_time=str(q["delivery_time"]),
        validity_days=int(q["validity_days"]),
        items=items,
        notes=q.get("notes"),
    )

    filename = f"cotizacion_{int(q['quote_no']):06d}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.get("/cotizacion/{quote_id}/editar", response_class=HTMLResponse)
def editar_get(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    engine = "postgres" if _using_postgres() else "sqlite"
    placeholder = "%s" if engine == "postgres" else "?"

    q = db_fetchone(f"SELECT * FROM quotes WHERE id={placeholder}", (quote_id,))
    if not q:
        return RedirectResponse(url="/historial", status_code=303)

    items = db_fetchall(f"SELECT * FROM quote_items WHERE quote_id={placeholder} ORDER BY id", (quote_id,))

    return templates.TemplateResponse("editar.html", {
        "request": request,
        "q": q,
        "items": items,
        "products": load_products(),
        "empresa": EMPRESA_NOMBRE,
        "telf": EMPRESA_TELF,
        "iva_rate": IVA_RATE,
    })


@app.post("/cotizacion/{quote_id}/editar")
def editar_post(
    request: Request,
    quote_id: int,
    client_name: str = Form(...),
    delivery_time: str = Form(...),
    validity_days: int = Form(...),
    notes: str = Form(""),
    item_sku: List[str] = Form([]),
    item_name: List[str] = Form([]),
    item_unit: List[str] = Form([]),
    item_qty: List[float] = Form([]),
    item_unit_price: List[float] = Form([]),
):
    gate = require_login(request)
    if gate:
        return gate

    items = []
    for i in range(len(item_name)):
        name = (item_name[i] or "").strip()
        if not name:
            continue
        qty = float(item_qty[i] or 0)
        price = float(item_unit_price[i] or 0)
        if qty <= 0:
            continue
        items.append({
            "sku": (item_sku[i] or "").strip(),
            "name": name,
            "unit": (item_unit[i] or "unidad").strip(),
            "qty": qty,
            "unit_price": price,
        })

    if not items:
        return RedirectResponse(url=f"/cotizacion/{quote_id}/editar?err=Agrega+al+menos+un+item", status_code=303)

    con, engine = db_connect()
    cur = con.cursor()

    if engine == "postgres":
        cur.execute(
            "UPDATE quotes SET client_name=%s, delivery_time=%s, validity_days=%s, notes=%s WHERE id=%s",
            (client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None, quote_id),
        )
        cur.execute("DELETE FROM quote_items WHERE quote_id=%s", (quote_id,))
        for it in items:
            cur.execute(
                "INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) "
                "VALUES(%s,%s,%s,%s,%s,%s)",
                (quote_id, it["sku"], it["name"], it["unit"], it["qty"], it["unit_price"]),
            )
    else:
        cur.execute(
            "UPDATE quotes SET client_name=?, delivery_time=?, validity_days=?, notes=? WHERE id=?",
            (client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None, quote_id),
        )
        cur.execute("DELETE FROM quote_items WHERE quote_id=?", (quote_id,))
        for it in items:
            cur.execute(
                "INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) "
                "VALUES(?,?,?,?,?,?)",
                (quote_id, it["sku"], it["name"], it["unit"], it["qty"], it["unit_price"]),
            )

    con.commit()
    con.close()
    return RedirectResponse(url="/historial", status_code=303)


@app.post("/cotizacion/{quote_id}/borrar")
def borrar(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    con, engine = db_connect()
    cur = con.cursor()

    if engine == "postgres":
        # ON DELETE CASCADE ya borra items, pero igual está bien explícito
        cur.execute("DELETE FROM quote_items WHERE quote_id=%s", (quote_id,))
        cur.execute("DELETE FROM quotes WHERE id=%s", (quote_id,))
    else:
        cur.execute("DELETE FROM quote_items WHERE quote_id=?", (quote_id,))
        cur.execute("DELETE FROM quotes WHERE id=?", (quote_id,))

    con.commit()
    con.close()
    return RedirectResponse(url="/historial", status_code=303)