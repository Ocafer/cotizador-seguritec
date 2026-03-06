from __future__ import annotations

import io
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Any, Tuple

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import load_workbook
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles
import secrets


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
DB_PATH = os.path.join(BASE_DIR, "app.db")
EXCEL_PATH = os.path.join(DATA_DIR, "precios.xlsx")

# Render / Postgres
DATABASE_URL = os.environ.get("DATABASE_URL")  # en Render ponla como DATABASE_URL

ADMIN_USER = os.environ.get("ADMIN_USER", "seguritec")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "cambia_esto")
SESSION_SECRET = os.environ.get("SESSION_SECRET", secrets.token_urlsafe(32))

# Logo (opcional). Si existe: static/logo.png
LOGO_PATH = os.environ.get("LOGO_PATH", os.path.join(STATIC_DIR, "logo.png"))

app = FastAPI(title=APP_TITLE)
templates = Jinja2Templates(directory=TEMPLATES_DIR)

app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# =========================
# DB backend selector
# =========================
IS_POSTGRES = bool(DATABASE_URL)

def _pg_connect():
    import psycopg
    from psycopg.rows import dict_row

    url = DATABASE_URL
    if url and "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}sslmode=require"

    return psycopg.connect(url, row_factory=dict_row)

def db_connect():
    """
    Devuelve conexión (sqlite o postgres).
    """
    if IS_POSTGRES:
        con = _pg_connect()
        return con
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def db_exec(sql: str, params: Tuple[Any, ...] = ()):
    """
    Ejecuta SQL de forma portable.
    En Postgres: placeholders %s
    En SQLite: placeholders ?
    """
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        con.commit()
        return cur
    finally:
        try:
            con.close()
        except Exception:
            pass

def db_fetchone(sql: str, params: Tuple[Any, ...] = ()):
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        try:
            con.close()
        except Exception:
            pass

def db_fetchall(sql: str, params: Tuple[Any, ...] = ()):
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        try:
            con.close()
        except Exception:
            pass

def psql(sql: str) -> str:
    """
    Convierte placeholders ? (sqlite style) a %s (postgres style).
    Es simple y suficiente para este proyecto (no uses ? en strings).
    """
    if not IS_POSTGRES:
        return sql
    return sql.replace("?", "%s")


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
# Catalog types
# =========================
@dataclass
class Product:
    sku: str
    categoria: str
    nombre: str
    unidad: str
    precio_bs: float
    activo: int = 1


# =========================
# Excel helpers (solo para import)
# =========================
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

def read_products_from_excel() -> List[Product]:
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

        products.append(Product(sku=sku, categoria=categoria, nombre=nombre, unidad=unidad, precio_bs=precio_bs, activo=1))

    products.sort(key=lambda p: (p.categoria.lower(), p.nombre.lower()))
    return products


# =========================
# DB schema
# =========================
def init_db() -> None:
    if IS_POSTGRES:
        # Postgres
        db_exec("""
            CREATE TABLE IF NOT EXISTS quotes (
                id SERIAL PRIMARY KEY,
                quote_no INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL,
                client_name TEXT NOT NULL,
                delivery_time TEXT NOT NULL,
                validity_days INTEGER NOT NULL,
                notes TEXT
            )
        """)
        db_exec("""
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
        db_exec("""
            CREATE TABLE IF NOT EXISTS counter (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        db_exec("""
            INSERT INTO counter(key, value)
            VALUES('quote_no', 0)
            ON CONFLICT (key) DO NOTHING
        """)
        db_exec("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                sku TEXT UNIQUE,
                categoria TEXT NOT NULL DEFAULT '',
                nombre TEXT NOT NULL,
                unidad TEXT NOT NULL DEFAULT 'unidad',
                precio_bs DOUBLE PRECISION NOT NULL DEFAULT 0,
                activo INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
    else:
        # SQLite
        con = db_connect()
        try:
            cur = con.cursor()
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
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
            """)
            cur.execute("INSERT OR IGNORE INTO counter(key, value) VALUES('quote_no', 0)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT UNIQUE,
                    categoria TEXT NOT NULL DEFAULT '',
                    nombre TEXT NOT NULL,
                    unidad TEXT NOT NULL DEFAULT 'unidad',
                    precio_bs REAL NOT NULL DEFAULT 0,
                    activo INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
            """)
            con.commit()
        finally:
            con.close()

def products_count() -> int:
    row = db_fetchone("SELECT COUNT(*) AS total FROM products")
    return int(row["total"]) if row else 0

def seed_products_from_excel_if_empty():
    if products_count() > 0:
        return
    if not os.path.exists(EXCEL_PATH):
        return

    try:
        products = read_products_from_excel()
    except Exception:
        return

    if not products:
        return

    if IS_POSTGRES:
        for p in products:
            db_exec(
                psql("""INSERT INTO products(sku,categoria,nombre,unidad,precio_bs,activo)
                        VALUES(?,?,?,?,?,?)
                        ON CONFLICT (sku) DO UPDATE SET
                          categoria=EXCLUDED.categoria,
                          nombre=EXCLUDED.nombre,
                          unidad=EXCLUDED.unidad,
                          precio_bs=EXCLUDED.precio_bs,
                          activo=EXCLUDED.activo
                """),
                (p.sku or None, p.categoria, p.nombre, p.unidad, p.precio_bs, p.activo),
            )
    else:
        con = db_connect()
        try:
            cur = con.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for p in products:
                cur.execute("""
                    INSERT OR REPLACE INTO products(sku,categoria,nombre,unidad,precio_bs,activo,created_at)
                    VALUES(?,?,?,?,?,?,?)
                """, (p.sku or None, p.categoria, p.nombre, p.unidad, p.precio_bs, p.activo, now))
            con.commit()
        finally:
            con.close()

def load_products() -> List[Product]:
    rows = db_fetchall("""
        SELECT sku, categoria, nombre, unidad, precio_bs, activo
        FROM products
        WHERE activo=1
        ORDER BY categoria, nombre
    """)
    products: List[Product] = []
    for r in rows:
        products.append(Product(
            sku=(r["sku"] or "").strip() if r["sku"] else "",
            categoria=str(r["categoria"] or ""),
            nombre=str(r["nombre"] or ""),
            unidad=str(r["unidad"] or "unidad"),
            precio_bs=float(r["precio_bs"] or 0),
        ))
    return products


init_db()
seed_products_from_excel_if_empty()


def next_quote_no() -> int:
    if IS_POSTGRES:
        db_exec("UPDATE counter SET value = value + 1 WHERE key='quote_no'")
        row = db_fetchone("SELECT value FROM counter WHERE key='quote_no'")
        return int(row["value"])
    else:
        con = db_connect()
        try:
            cur = con.cursor()
            cur.execute("UPDATE counter SET value = value + 1 WHERE key='quote_no'")
            cur.execute("SELECT value FROM counter WHERE key='quote_no'")
            n = cur.fetchone()[0]
            con.commit()
            return int(n)
        finally:
            con.close()


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

    # Logo opcional
    if LOGO_PATH and os.path.exists(LOGO_PATH):
        try:
            c.drawImage(LOGO_PATH, x0, y - 12 * mm, width=30 * mm, height=12 * mm, mask='auto')
        except Exception:
            pass

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x0 + (35 * mm if (LOGO_PATH and os.path.exists(LOGO_PATH)) else 0), y, EMPRESA_NOMBRE)
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
    if username == ADMIN_USER and password == ADMIN_PASS:
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

    if IS_POSTGRES:
        created_at = datetime.now()
        con = db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                psql("INSERT INTO quotes(quote_no, created_at, client_name, delivery_time, validity_days, notes) VALUES(?,?,?,?,?,?) RETURNING id"),
                (qno, created_at, client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None),
            )
            quote_id = int(cur.fetchone()["id"])

            for it in items:
                cur.execute(
                    psql("INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) VALUES(?,?,?,?,?,?)"),
                    (quote_id, it["sku"] or None, it["name"], it["unit"], it["qty"], it["unit_price"]),
                )

            con.commit()
        finally:
            con.close()
    else:
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M")
        con = db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO quotes(quote_no, created_at, client_name, delivery_time, validity_days, notes) VALUES(?,?,?,?,?,?)",
                (qno, created_at, client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None),
            )
            quote_id = cur.lastrowid

            for it in items:
                cur.execute(
                    "INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) VALUES(?,?,?,?,?,?)",
                    (quote_id, it["sku"], it["name"], it["unit"], it["qty"], it["unit_price"]),
                )
            con.commit()
        finally:
            con.close()

    return RedirectResponse(url=f"/cotizacion/{quote_id}/pdf", status_code=303)

@app.get("/historial", response_class=HTMLResponse)
def historial(request: Request):
    gate = require_login(request)
    if gate:
        return gate

    if IS_POSTGRES:
        rows = db_fetchall("SELECT id, quote_no, created_at, client_name, delivery_time, validity_days, notes FROM quotes ORDER BY id DESC LIMIT 500")
    else:
        rows = db_fetchall("SELECT * FROM quotes ORDER BY id DESC LIMIT 500")

    return templates.TemplateResponse("historial.html", {"request": request, "quotes": rows, "empresa": EMPRESA_NOMBRE})

@app.get("/cotizacion/{quote_id}/pdf")
def cotizacion_pdf(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    if IS_POSTGRES:
        q = db_fetchone("SELECT * FROM quotes WHERE id=%s", (quote_id,))
        if not q:
            return RedirectResponse(url="/historial", status_code=303)

        items_rows = db_fetchall(
            "SELECT sku, name, unit, qty, unit_price FROM quote_items WHERE quote_id=%s ORDER BY id",
            (quote_id,),
        )

        items = []
        for r in items_rows:
            items.append({
                "sku": (r["sku"] or ""),
                "name": r["name"],
                "unit": r["unit"],
                "qty": float(r["qty"]),
                "unit_price": float(r["unit_price"]),
            })

        created_at = q["created_at"].strftime("%Y-%m-%d %H:%M") if hasattr(q["created_at"], "strftime") else str(q["created_at"])

        pdf = generate_pdf(
            quote_no=int(q["quote_no"]),
            created_at=created_at,
            client_name=str(q["client_name"]),
            delivery_time=str(q["delivery_time"]),
            validity_days=int(q["validity_days"]),
            items=items,
            notes=q["notes"],
        )

        filename = f"cotizacion_{int(q['quote_no']):06d}.pdf"
        return StreamingResponse(
            io.BytesIO(pdf),
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{filename}"'},
        )

    # --- SQLite (tu código actual) ---
    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM quotes WHERE id=?", (quote_id,))
        q = cur.fetchone()
        if not q:
            return RedirectResponse(url="/historial", status_code=303)

        cur.execute("SELECT * FROM quote_items WHERE quote_id=? ORDER BY id", (quote_id,))
        items = [dict(r) for r in cur.fetchall()]
    finally:
        con.close()

    pdf = generate_pdf(
        quote_no=int(q["quote_no"]),
        created_at=str(q["created_at"]),
        client_name=str(q["client_name"]),
        delivery_time=str(q["delivery_time"]),
        validity_days=int(q["validity_days"]),
        items=items,
        notes=q["notes"],
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

    if IS_POSTGRES:
        q = db_fetchone("SELECT * FROM quotes WHERE id=%s", (quote_id,))
        if not q:
            return RedirectResponse(url="/historial", status_code=303)

        items_rows = db_fetchall("SELECT id, sku, name, unit, qty, unit_price FROM quote_items WHERE quote_id=%s ORDER BY id", (quote_id,))
        return templates.TemplateResponse("editar.html", {
            "request": request,
            "q": q,
            "items": items_rows,
            "products": load_products(),
            "empresa": EMPRESA_NOMBRE,
            "telf": EMPRESA_TELF,
            "iva_rate": IVA_RATE,
        })

    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM quotes WHERE id=?", (quote_id,))
        q = cur.fetchone()
        cur.execute("SELECT * FROM quote_items WHERE quote_id=? ORDER BY id", (quote_id,))
        items = cur.fetchall()
    finally:
        con.close()

    if not q:
        return RedirectResponse(url="/historial", status_code=303)

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

    if IS_POSTGRES:
        con = db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                psql("""UPDATE quotes SET client_name=?, delivery_time=?, validity_days=?, notes=? WHERE id=?"""),
                (client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None, quote_id),
            )
            cur.execute("DELETE FROM quote_items WHERE quote_id=%s", (quote_id,))
            for it in items:
                cur.execute(
                    psql("INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) VALUES(?,?,?,?,?,?)"),
                    (quote_id, it["sku"] or None, it["name"], it["unit"], it["qty"], it["unit_price"]),
                )
            con.commit()
        finally:
            con.close()
        return RedirectResponse(url="/historial", status_code=303)

    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute("""
            UPDATE quotes
            SET client_name=?, delivery_time=?, validity_days=?, notes=?
            WHERE id=?
        """, (client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None, quote_id))

        cur.execute("DELETE FROM quote_items WHERE quote_id=?", (quote_id,))
        for it in items:
            cur.execute(
                "INSERT INTO quote_items(quote_id, sku, name, unit, qty, unit_price) VALUES(?,?,?,?,?,?)",
                (quote_id, it["sku"], it["name"], it["unit"], it["qty"], it["unit_price"]),
            )
        con.commit()
    finally:
        con.close()

    return RedirectResponse(url="/historial", status_code=303)

@app.post("/cotizacion/{quote_id}/borrar")
def borrar(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    if IS_POSTGRES:
        con = db_connect()
        try:
            cur = con.cursor()
            cur.execute("DELETE FROM quotes WHERE id=%s", (quote_id,))
            con.commit()
        finally:
            con.close()
        return RedirectResponse(url="/historial", status_code=303)

    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM quote_items WHERE quote_id=?", (quote_id,))
        cur.execute("DELETE FROM quotes WHERE id=?", (quote_id,))
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/historial", status_code=303)


# =========================
# Gestión de productos
# =========================
@dataclass
class ProductoRow:
    id: int
    sku: str
    categoria: str
    nombre: str
    unidad: str
    precio_bs: float
    activo: int

def load_all_products() -> List[ProductoRow]:
    if IS_POSTGRES:
        rows = db_fetchall("SELECT id, sku, categoria, nombre, unidad, precio_bs, activo FROM products ORDER BY categoria, nombre")
    else:
        rows = db_fetchall("SELECT id, sku, categoria, nombre, unidad, precio_bs, activo FROM products ORDER BY categoria, nombre")
    result = []
    for r in rows:
        result.append(ProductoRow(
            id=int(r["id"]),
            sku=str(r["sku"] or ""),
            categoria=str(r["categoria"] or ""),
            nombre=str(r["nombre"] or ""),
            unidad=str(r["unidad"] or "unidad"),
            precio_bs=float(r["precio_bs"] or 0),
            activo=int(r["activo"]),
        ))
    return result

@app.get("/productos", response_class=HTMLResponse)
def productos_get(request: Request, msg: str = "", msg_type: str = "success"):
    gate = require_login(request)
    if gate:
        return gate
    productos = load_all_products()
    return templates.TemplateResponse("productos.html", {
        "request": request,
        "productos": productos,
        "empresa": EMPRESA_NOMBRE,
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
    })

@app.post("/productos/guardar")
def productos_guardar(
    request: Request,
    producto_id: str = Form(""),
    sku: str = Form(""),
    categoria: str = Form(...),
    nombre: str = Form(...),
    unidad: str = Form(...),
    precio_bs: float = Form(...),
):
    gate = require_login(request)
    if gate:
        return gate

    sku = sku.strip() or None
    categoria = categoria.strip()
    nombre = nombre.strip()
    unidad = unidad.strip()

    if producto_id:
        # Editar existente
        if IS_POSTGRES:
            db_exec(
                "UPDATE products SET sku=%s, categoria=%s, nombre=%s, unidad=%s, precio_bs=%s WHERE id=%s",
                (sku, categoria, nombre, unidad, precio_bs, int(producto_id)),
            )
        else:
            db_exec(
                "UPDATE products SET sku=?, categoria=?, nombre=?, unidad=?, precio_bs=? WHERE id=?",
                (sku, categoria, nombre, unidad, precio_bs, int(producto_id)),
            )
        msg = "Producto actualizado correctamente."
    else:
        # Nuevo producto
        if IS_POSTGRES:
            db_exec(
                "INSERT INTO products(sku, categoria, nombre, unidad, precio_bs, activo) VALUES(%s,%s,%s,%s,%s,1)",
                (sku, categoria, nombre, unidad, precio_bs),
            )
        else:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db_exec(
                "INSERT INTO products(sku, categoria, nombre, unidad, precio_bs, activo, created_at) VALUES(?,?,?,?,?,1,?)",
                (sku, categoria, nombre, unidad, precio_bs, now),
            )
        msg = "Producto agregado correctamente."

    return RedirectResponse(url=f"/productos?msg={msg}&msg_type=success", status_code=303)

@app.post("/productos/{producto_id}/toggle")
def productos_toggle(request: Request, producto_id: int):
    gate = require_login(request)
    if gate:
        return gate

    if IS_POSTGRES:
        db_exec("UPDATE products SET activo = CASE WHEN activo=1 THEN 0 ELSE 1 END WHERE id=%s", (producto_id,))
    else:
        db_exec("UPDATE products SET activo = CASE WHEN activo=1 THEN 0 ELSE 1 END WHERE id=?", (producto_id,))

    return RedirectResponse(url="/productos?msg=Estado+actualizado.&msg_type=success", status_code=303)

@app.post("/productos/{producto_id}/borrar")
def productos_borrar(request: Request, producto_id: int):
    gate = require_login(request)
    if gate:
        return gate

    if IS_POSTGRES:
        db_exec("DELETE FROM products WHERE id=%s", (producto_id,))
    else:
        db_exec("DELETE FROM products WHERE id=?", (producto_id,))

    return RedirectResponse(url="/productos?msg=Producto+eliminado.&msg_type=warning", status_code=303)


# =========================
# Instalaciones / Agenda
# =========================
@dataclass
class InstalacionRow:
    id: int
    quote_id: int
    quote_no: int
    client_name: str
    fecha_instalacion: str
    tecnico: str
    estado: str
    notas_instalacion: str
    total_con_iva: float

def init_instalaciones_table():
    if IS_POSTGRES:
        db_exec("""
            CREATE TABLE IF NOT EXISTS instalaciones (
                id SERIAL PRIMARY KEY,
                quote_id INTEGER NOT NULL REFERENCES quotes(id) ON DELETE CASCADE,
                fecha_instalacion DATE NOT NULL,
                tecnico TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'pendiente',
                notas_instalacion TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        # Tabla técnicos
        db_exec("""
            CREATE TABLE IF NOT EXISTS tecnicos (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                telefono TEXT,
                especialidad TEXT,
                activo INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        # Tabla relación instalacion <-> técnicos (múltiples)
        db_exec("""
            CREATE TABLE IF NOT EXISTS instalacion_tecnicos (
                id SERIAL PRIMARY KEY,
                instalacion_id INTEGER NOT NULL REFERENCES instalaciones(id) ON DELETE CASCADE,
                tecnico_id INTEGER,
                tecnico_nombre TEXT NOT NULL
            )
        """)
    else:
        db_exec("""
            CREATE TABLE IF NOT EXISTS instalaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id INTEGER NOT NULL,
                fecha_instalacion TEXT NOT NULL,
                tecnico TEXT NOT NULL,
                estado TEXT NOT NULL DEFAULT 'pendiente',
                notas_instalacion TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (quote_id) REFERENCES quotes(id)
            )
        """)
        db_exec("""
            CREATE TABLE IF NOT EXISTS tecnicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                telefono TEXT,
                especialidad TEXT,
                activo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)
        db_exec("""
            CREATE TABLE IF NOT EXISTS instalacion_tecnicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instalacion_id INTEGER NOT NULL,
                tecnico_id INTEGER,
                tecnico_nombre TEXT NOT NULL,
                FOREIGN KEY (instalacion_id) REFERENCES instalaciones(id)
            )
        """)

init_instalaciones_table()

def get_quote_total(quote_id: int) -> float:
    if IS_POSTGRES:
        rows = db_fetchall("SELECT qty, unit_price FROM quote_items WHERE quote_id=%s", (quote_id,))
    else:
        rows = db_fetchall("SELECT qty, unit_price FROM quote_items WHERE quote_id=?", (quote_id,))
    subtotal = sum(float(r["qty"]) * float(r["unit_price"]) for r in rows)
    return subtotal * (1 + IVA_RATE)

# =========================
# Técnicos helpers y rutas
# =========================
@dataclass
class TecnicoRow:
    id: int
    nombre: str
    telefono: str
    especialidad: str
    activo: int
    total_instalaciones: int = 0

def load_tecnicos_activos() -> List[TecnicoRow]:
    rows = db_fetchall("SELECT id, nombre, telefono, especialidad, activo FROM tecnicos WHERE activo=1 ORDER BY nombre")
    return [TecnicoRow(id=int(r["id"]), nombre=str(r["nombre"]), telefono=str(r["telefono"] or ""),
                       especialidad=str(r["especialidad"] or ""), activo=1) for r in rows]

def load_all_tecnicos() -> List[TecnicoRow]:
    if IS_POSTGRES:
        rows = db_fetchall("""
            SELECT t.id, t.nombre, t.telefono, t.especialidad, t.activo,
                   COUNT(it.id) AS total_instalaciones
            FROM tecnicos t
            LEFT JOIN instalacion_tecnicos it ON it.tecnico_id = t.id
            GROUP BY t.id, t.nombre, t.telefono, t.especialidad, t.activo
            ORDER BY t.nombre
        """)
    else:
        rows = db_fetchall("""
            SELECT t.id, t.nombre, t.telefono, t.especialidad, t.activo,
                   COUNT(it.id) AS total_instalaciones
            FROM tecnicos t
            LEFT JOIN instalacion_tecnicos it ON it.tecnico_id = t.id
            GROUP BY t.id
            ORDER BY t.nombre
        """)
    return [TecnicoRow(id=int(r["id"]), nombre=str(r["nombre"]), telefono=str(r["telefono"] or ""),
                       especialidad=str(r["especialidad"] or ""), activo=int(r["activo"]),
                       total_instalaciones=int(r["total_instalaciones"])) for r in rows]

@app.get("/tecnicos", response_class=HTMLResponse)
def tecnicos_get(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    return templates.TemplateResponse("tecnicos.html", {
        "request": request,
        "empresa": EMPRESA_NOMBRE,
        "tecnicos": load_all_tecnicos(),
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
    })

@app.post("/tecnicos/guardar")
def tecnicos_guardar(
    request: Request,
    tecnico_id: str = Form(""),
    nombre: str = Form(...),
    telefono: str = Form(""),
    especialidad: str = Form(""),
    activo: int = Form(1),
):
    gate = require_login(request)
    if gate:
        return gate

    nombre = nombre.strip()
    telefono = telefono.strip() or None
    especialidad = especialidad.strip() or None

    if tecnico_id:
        if IS_POSTGRES:
            db_exec("UPDATE tecnicos SET nombre=%s, telefono=%s, especialidad=%s, activo=%s WHERE id=%s",
                    (nombre, telefono, especialidad, activo, int(tecnico_id)))
        else:
            db_exec("UPDATE tecnicos SET nombre=?, telefono=?, especialidad=?, activo=? WHERE id=?",
                    (nombre, telefono, especialidad, activo, int(tecnico_id)))
        msg = "Técnico+actualizado."
    else:
        if IS_POSTGRES:
            db_exec("INSERT INTO tecnicos(nombre, telefono, especialidad, activo) VALUES(%s,%s,%s,%s)",
                    (nombre, telefono, especialidad, activo))
        else:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db_exec("INSERT INTO tecnicos(nombre, telefono, especialidad, activo, created_at) VALUES(?,?,?,?,?)",
                    (nombre, telefono, especialidad, activo, now))
        msg = "Técnico+agregado."

    return RedirectResponse(url=f"/tecnicos?msg={msg}&msg_type=success", status_code=303)

@app.post("/tecnicos/{tecnico_id}/borrar")
def tecnicos_borrar(request: Request, tecnico_id: int):
    gate = require_login(request)
    if gate:
        return gate
    if IS_POSTGRES:
        db_exec("DELETE FROM tecnicos WHERE id=%s", (tecnico_id,))
    else:
        db_exec("DELETE FROM tecnicos WHERE id=?", (tecnico_id,))
    return RedirectResponse(url="/tecnicos?msg=Técnico+eliminado.&msg_type=warning", status_code=303)


@app.get("/instalacion/{quote_id}/agendar", response_class=HTMLResponse)
def agendar_get(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    if IS_POSTGRES:
        q = db_fetchone("SELECT * FROM quotes WHERE id=%s", (quote_id,))
        inst = db_fetchone("SELECT * FROM instalaciones WHERE quote_id=%s", (quote_id,))
    else:
        q = db_fetchone("SELECT * FROM quotes WHERE id=?", (quote_id,))
        inst = db_fetchone("SELECT * FROM instalaciones WHERE quote_id=?", (quote_id,))

    if not q:
        return RedirectResponse(url="/historial", status_code=303)

    # Técnicos ya asignados a esta instalación
    tecnicos_asignados = []
    if inst:
        if IS_POSTGRES:
            tecs = db_fetchall("SELECT tecnico_id, tecnico_nombre FROM instalacion_tecnicos WHERE instalacion_id=%s", (inst["id"],))
        else:
            tecs = db_fetchall("SELECT tecnico_id, tecnico_nombre FROM instalacion_tecnicos WHERE instalacion_id=?", (inst["id"],))
        tecnicos_asignados = [{"id": r["tecnico_id"], "nombre": r["tecnico_nombre"]} for r in tecs]

    # Técnicos activos para el select
    tecs_activos = load_tecnicos_activos()

    import json
    return templates.TemplateResponse("agendar.html", {
        "request": request,
        "q": q,
        "instalacion": inst,
        "empresa": EMPRESA_NOMBRE,
        "tecnicos_activos": tecs_activos,
        "tecnicos_asignados_json": json.dumps(tecnicos_asignados),
    })

@app.post("/instalacion/{quote_id}/guardar")
def agendar_post(
    request: Request,
    quote_id: int,
    fecha_instalacion: str = Form(...),
    tecnicos_json: str = Form("[]"),
    estado: str = Form("pendiente"),
    notas_instalacion: str = Form(""),
):
    gate = require_login(request)
    if gate:
        return gate

    import json as _json
    try:
        tecs = _json.loads(tecnicos_json)
    except Exception:
        tecs = []

    # Nombre resumido para campo legado "tecnico"
    tecnico_str = ", ".join(t["nombre"] for t in tecs) if tecs else "Sin asignar"

    if IS_POSTGRES:
        existing = db_fetchone("SELECT id FROM instalaciones WHERE quote_id=%s", (quote_id,))
        if existing:
            inst_id = int(existing["id"])
            db_exec(
                "UPDATE instalaciones SET fecha_instalacion=%s, tecnico=%s, estado=%s, notas_instalacion=%s WHERE id=%s",
                (fecha_instalacion, tecnico_str, estado, notas_instalacion.strip() or None, inst_id),
            )
            db_exec("DELETE FROM instalacion_tecnicos WHERE instalacion_id=%s", (inst_id,))
        else:
            con = db_connect()
            try:
                cur = con.cursor()
                cur.execute(
                    "INSERT INTO instalaciones(quote_id, fecha_instalacion, tecnico, estado, notas_instalacion) VALUES(%s,%s,%s,%s,%s) RETURNING id",
                    (quote_id, fecha_instalacion, tecnico_str, estado, notas_instalacion.strip() or None),
                )
                inst_id = int(cur.fetchone()["id"])
                con.commit()
            finally:
                con.close()
        for t in tecs:
            db_exec(
                "INSERT INTO instalacion_tecnicos(instalacion_id, tecnico_id, tecnico_nombre) VALUES(%s,%s,%s)",
                (inst_id, t.get("id"), t["nombre"]),
            )
    else:
        existing = db_fetchone("SELECT id FROM instalaciones WHERE quote_id=?", (quote_id,))
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if existing:
            inst_id = int(existing["id"])
            db_exec(
                "UPDATE instalaciones SET fecha_instalacion=?, tecnico=?, estado=?, notas_instalacion=? WHERE id=?",
                (fecha_instalacion, tecnico_str, estado, notas_instalacion.strip() or None, inst_id),
            )
            db_exec("DELETE FROM instalacion_tecnicos WHERE instalacion_id=?", (inst_id,))
        else:
            con = db_connect()
            try:
                cur = con.cursor()
                cur.execute(
                    "INSERT INTO instalaciones(quote_id, fecha_instalacion, tecnico, estado, notas_instalacion, created_at) VALUES(?,?,?,?,?,?)",
                    (quote_id, fecha_instalacion, tecnico_str, estado, notas_instalacion.strip() or None, now),
                )
                inst_id = cur.lastrowid
                con.commit()
            finally:
                con.close()
        for t in tecs:
            db_exec(
                "INSERT INTO instalacion_tecnicos(instalacion_id, tecnico_id, tecnico_nombre) VALUES(?,?,?)",
                (inst_id, t.get("id"), t["nombre"]),
            )

    return RedirectResponse(url="/agenda", status_code=303)

@app.get("/agenda", response_class=HTMLResponse)
def agenda(request: Request, fecha: str = ""):
    gate = require_login(request)
    if gate:
        return gate

    fecha_sel = fecha or datetime.now().strftime("%Y-%m-%d")

    if IS_POSTGRES:
        rows_dia = db_fetchall("""
            SELECT i.quote_id, i.fecha_instalacion, i.tecnico, i.estado, i.notas_instalacion,
                   q.quote_no, q.client_name
            FROM instalaciones i
            JOIN quotes q ON q.id = i.quote_id
            WHERE i.fecha_instalacion = %s
            ORDER BY i.tecnico
        """, (fecha_sel,))
        rows_proximas = db_fetchall("""
            SELECT i.quote_id, i.fecha_instalacion, i.tecnico, i.estado,
                   q.quote_no, q.client_name
            FROM instalaciones i
            JOIN quotes q ON q.id = i.quote_id
            WHERE i.fecha_instalacion > %s AND i.estado != 'completada'
            ORDER BY i.fecha_instalacion
            LIMIT 20
        """, (fecha_sel,))
    else:
        rows_dia = db_fetchall("""
            SELECT i.quote_id, i.fecha_instalacion, i.tecnico, i.estado, i.notas_instalacion,
                   q.quote_no, q.client_name
            FROM instalaciones i
            JOIN quotes q ON q.id = i.quote_id
            WHERE i.fecha_instalacion = ?
            ORDER BY i.tecnico
        """, (fecha_sel,))
        rows_proximas = db_fetchall("""
            SELECT i.quote_id, i.fecha_instalacion, i.tecnico, i.estado,
                   q.quote_no, q.client_name
            FROM instalaciones i
            JOIN quotes q ON q.id = i.quote_id
            WHERE i.fecha_instalacion > ? AND i.estado != 'completada'
            ORDER BY i.fecha_instalacion
            LIMIT 20
        """, (fecha_sel,))

    def enrich(rows):
        result = []
        for r in rows:
            total = get_quote_total(int(r["quote_id"]))
            result.append({
                "quote_id": r["quote_id"],
                "quote_no": r["quote_no"],
                "client_name": r["client_name"],
                "fecha_instalacion": str(r["fecha_instalacion"]),
                "tecnico": r["tecnico"],
                "estado": r["estado"],
                "notas_instalacion": r.get("notas_instalacion", ""),
                "total_con_iva": total,
            })
        return result

    instalaciones_dia = enrich(rows_dia)
    proximas = enrich(rows_proximas)

    return templates.TemplateResponse("agenda.html", {
        "request": request,
        "empresa": EMPRESA_NOMBRE,
        "fecha_sel": fecha_sel,
        "instalaciones_dia": instalaciones_dia,
        "proximas": proximas,
        "total_dia": len(instalaciones_dia),
    })

@app.get("/reportes", response_class=HTMLResponse)
def reportes(request: Request, desde: str = "", hasta: str = "", tecnico: str = ""):
    gate = require_login(request)
    if gate:
        return gate

    hoy = datetime.now().strftime("%Y-%m-%d")
    desde = desde or datetime.now().strftime("%Y-%m-01")
    hasta = hasta or hoy

    if IS_POSTGRES:
        rows = db_fetchall("""
            SELECT i.quote_id, i.fecha_instalacion, i.tecnico, i.estado,
                   q.quote_no, q.client_name
            FROM instalaciones i
            JOIN quotes q ON q.id = i.quote_id
            WHERE i.fecha_instalacion BETWEEN %s AND %s
            ORDER BY i.fecha_instalacion
        """, (desde, hasta))
        tecnicos_rows = db_fetchall("SELECT DISTINCT tecnico FROM instalaciones ORDER BY tecnico")
    else:
        rows = db_fetchall("""
            SELECT i.quote_id, i.fecha_instalacion, i.tecnico, i.estado,
                   q.quote_no, q.client_name
            FROM instalaciones i
            JOIN quotes q ON q.id = i.quote_id
            WHERE i.fecha_instalacion BETWEEN ? AND ?
            ORDER BY i.fecha_instalacion
        """, (desde, hasta))
        tecnicos_rows = db_fetchall("SELECT DISTINCT tecnico FROM instalaciones ORDER BY tecnico")

    tecnicos = [r["tecnico"] for r in tecnicos_rows]

    instalaciones = []
    for r in rows:
        if tecnico and r["tecnico"] != tecnico:
            continue
        total = get_quote_total(int(r["quote_id"]))
        instalaciones.append({
            "quote_id": r["quote_id"],
            "quote_no": r["quote_no"],
            "client_name": r["client_name"],
            "fecha_instalacion": str(r["fecha_instalacion"]),
            "tecnico": r["tecnico"],
            "estado": r["estado"],
            "total_con_iva": total,
        })

    total_con_iva = sum(i["total_con_iva"] for i in instalaciones)
    total_sin_iva = total_con_iva / (1 + IVA_RATE)
    total_iva = total_con_iva - total_sin_iva

    stats = {
        "total": len(instalaciones),
        "pendientes": sum(1 for i in instalaciones if i["estado"] == "pendiente"),
        "en_curso": sum(1 for i in instalaciones if i["estado"] == "en_curso"),
        "completadas": sum(1 for i in instalaciones if i["estado"] == "completada"),
        "total_sin_iva": total_sin_iva,
        "total_iva": total_iva,
        "total_con_iva": total_con_iva,
    }

    return templates.TemplateResponse("reportes.html", {
        "request": request,
        "empresa": EMPRESA_NOMBRE,
        "desde": desde,
        "hasta": hasta,
        "tecnico_filtro": tecnico,
        "tecnicos": tecnicos,
        "instalaciones": instalaciones,
        "stats": stats,
    })