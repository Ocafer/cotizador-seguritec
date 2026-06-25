from __future__ import annotations
import io
import datetime as dt
from datetime import datetime
from typing import List

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from auth import is_logged_in, require_login
from config import EMPRESA_NOMBRE, EMPRESA_TELF, IVA_RATE
from database import db_connect, db_fetchone, db_fetchall, db_insert, db_exec, IS_POSTGRES, psql
from pdf import generate_pdf
from schema import next_quote_no
from services import load_products, get_quote_total
from templating import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return RedirectResponse(url="/dashboard", status_code=303)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    gate = require_login(request)
    if gate:
        return gate

    hoy = datetime.now()
    hoy_str = hoy.strftime("%Y-%m-%d")
    mes_actual = hoy.strftime("%Y-%m")

    row_inst = db_fetchone("""
        SELECT COUNT(*) AS total,
          SUM(CASE WHEN estado='pendiente' THEN 1 ELSE 0 END) AS pendientes,
          SUM(CASE WHEN estado='en_curso' THEN 1 ELSE 0 END) AS en_curso,
          SUM(CASE WHEN estado='completada' THEN 1 ELSE 0 END) AS completadas
        FROM instalaciones
    """)

    total_inst = int(row_inst["total"] or 0) if row_inst else 0
    pendientes = int(row_inst["pendientes"] or 0) if row_inst else 0
    en_curso = int(row_inst["en_curso"] or 0) if row_inst else 0
    completadas = int(row_inst["completadas"] or 0) if row_inst else 0

    # Month filter differs: postgres uses date range, sqlite uses substr
    if IS_POSTGRES:
        rows_mes = db_fetchall(
            "SELECT id FROM quotes WHERE created_at >= %s AND created_at < %s",
            (f"{mes_actual}-01",
             f"{hoy.year}-{hoy.month+1:02d}-01" if hoy.month < 12 else f"{hoy.year+1}-01-01"),
        )
    else:
        rows_mes = db_fetchall(
            "SELECT id FROM quotes WHERE substr(created_at,1,7) = ?", (mes_actual,)
        )

    ventas_mes = sum(get_quote_total(int(r["id"])) for r in rows_mes)
    ventas_mes_sin_iva = ventas_mes / (1 + IVA_RATE)

    fecha_limite = (hoy + dt.timedelta(days=7)).strftime("%Y-%m-%d")
    proximas_rows = db_fetchall(psql("""
        SELECT i.estado, i.tecnico, i.fecha_instalacion, q.client_name, q.id as quote_id
        FROM instalaciones i JOIN quotes q ON q.id = i.quote_id
        WHERE i.fecha_instalacion BETWEEN ? AND ?
        ORDER BY i.fecha_instalacion LIMIT 8
    """), (hoy_str, fecha_limite))
    proximas = [dict(r) for r in proximas_rows]

    cots_rows = db_fetchall(
        "SELECT id, quote_no, client_name, created_at FROM quotes ORDER BY id DESC LIMIT 6"
    )
    cotizaciones_recientes = []
    for r in cots_rows:
        ca = r["created_at"]
        ca_str = ca.strftime("%d/%m/%Y") if hasattr(ca, "strftime") else str(ca)[:10]
        cotizaciones_recientes.append({
            "id": int(r["id"]), "quote_no": int(r["quote_no"]),
            "client_name": str(r["client_name"]), "created_at_str": ca_str,
        })

    meses_labels = []
    meses_valores = []
    for i in range(5, -1, -1):
        d = hoy - dt.timedelta(days=30 * i)
        ym = d.strftime("%Y-%m")
        label = d.strftime("%b %Y")
        if IS_POSTGRES:
            primer_dia = dt.date(d.year, d.month, 1)
            ultimo_dia = dt.date(d.year + 1, 1, 1) if d.month == 12 else dt.date(d.year, d.month + 1, 1)
            rows_m = db_fetchall(
                "SELECT id FROM quotes WHERE created_at >= %s AND created_at < %s",
                (str(primer_dia), str(ultimo_dia)),
            )
        else:
            rows_m = db_fetchall(
                "SELECT id FROM quotes WHERE substr(created_at,1,7) = ?", (ym,)
            )
        meses_labels.append(label)
        meses_valores.append(round(sum(get_quote_total(int(r["id"])) for r in rows_m), 2))

    meses_nombres = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                     "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

    return templates.TemplateResponse("dashboard.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "hoy": hoy.strftime("%d/%m/%Y"), "mes_nombre": meses_nombres[hoy.month - 1],
        "stats": {
            "pendientes": pendientes, "en_curso": en_curso,
            "completadas": completadas, "total_instalaciones": total_inst,
            "ventas_mes_con_iva": ventas_mes, "ventas_mes_sin_iva": ventas_mes_sin_iva,
            "cotizaciones_mes": len(rows_mes),
        },
        "proximas_instalaciones": proximas,
        "cotizaciones_recientes": cotizaciones_recientes,
        "meses_labels": meses_labels,
        "meses_valores": meses_valores,
    })


@router.get("/nueva", response_class=HTMLResponse)
def nueva(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    return templates.TemplateResponse("nueva.html", {
        "request": request, "products": load_products(),
        "empresa": EMPRESA_NOMBRE, "telf": EMPRESA_TELF, "iva_rate": IVA_RATE,
    })


@router.post("/crear")
def crear_cotizacion(
    request: Request,
    client_name: str = Form(...),
    delivery_time: str = Form(...),
    validity_days: int = Form(...),
    notes: str = Form(""),
    fecha_cotizacion: str = Form(""),
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
        if qty <= 0:
            continue
        items.append({
            "sku": (item_sku[i] or "").strip(),
            "name": name,
            "unit": (item_unit[i] or "unidad").strip(),
            "qty": qty,
            "unit_price": float(item_unit_price[i] or 0),
        })

    if not items:
        return RedirectResponse(url="/nueva?err=Agrega+al+menos+un+item", status_code=303)

    qno = next_quote_no()
    # PostgreSQL accepts a datetime object; SQLite stores it as TEXT string
    created_at = datetime.now() if IS_POSTGRES else datetime.now().strftime("%Y-%m-%d %H:%M")
    if fecha_cotizacion:
        try:
            parsed = datetime.strptime(fecha_cotizacion, "%Y-%m-%d")
            created_at = parsed if IS_POSTGRES else parsed.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass

    quote_id = db_insert(
        "INSERT INTO quotes(quote_no,created_at,client_name,delivery_time,validity_days,notes) VALUES(?,?,?,?,?,?)",
        (qno, created_at, client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None),
    )
    for it in items:
        db_exec(
            psql("INSERT INTO quote_items(quote_id,sku,name,unit,qty,unit_price) VALUES(?,?,?,?,?,?)"),
            (quote_id, it["sku"] or None, it["name"], it["unit"], it["qty"], it["unit_price"]),
        )

    return RedirectResponse(url=f"/cotizacion/{quote_id}/pdf", status_code=303)


@router.get("/historial", response_class=HTMLResponse)
def historial(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    rows = db_fetchall(
        "SELECT id,quote_no,created_at,client_name,delivery_time,validity_days,notes FROM quotes ORDER BY id DESC LIMIT 500"
    )
    return templates.TemplateResponse("historial.html", {
        "request": request, "quotes": rows, "empresa": EMPRESA_NOMBRE,
    })


@router.get("/cotizacion/{quote_id}/pdf")
def cotizacion_pdf(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    q = db_fetchone(psql("SELECT * FROM quotes WHERE id=?"), (quote_id,))
    if not q:
        return RedirectResponse(url="/historial", status_code=303)
    items_rows = db_fetchall(
        psql("SELECT sku,name,unit,qty,unit_price FROM quote_items WHERE quote_id=? ORDER BY id"),
        (quote_id,),
    )
    items = [{"sku": r["sku"] or "", "name": r["name"], "unit": r["unit"],
              "qty": float(r["qty"]), "unit_price": float(r["unit_price"])} for r in items_rows]
    created_at = q["created_at"].strftime("%Y-%m-%d %H:%M") if hasattr(q["created_at"], "strftime") else str(q["created_at"])

    pdf_bytes = generate_pdf(
        quote_no=int(q["quote_no"]), created_at=created_at,
        client_name=str(q["client_name"]), delivery_time=str(q["delivery_time"]),
        validity_days=int(q["validity_days"]), items=items, notes=q["notes"],
    )
    filename = f"cotizacion_{int(q['quote_no']):06d}.pdf"
    return StreamingResponse(io.BytesIO(pdf_bytes), media_type="application/pdf",
                             headers={"Content-Disposition": f'inline; filename="{filename}"'})


@router.get("/cotizacion/{quote_id}/editar", response_class=HTMLResponse)
def editar_get(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    q = db_fetchone(psql("SELECT * FROM quotes WHERE id=?"), (quote_id,))
    if not q:
        return RedirectResponse(url="/historial", status_code=303)
    items_rows = db_fetchall(
        psql("SELECT id,sku,name,unit,qty,unit_price FROM quote_items WHERE quote_id=? ORDER BY id"),
        (quote_id,),
    )
    return templates.TemplateResponse("editar.html", {
        "request": request, "q": q, "items": items_rows,
        "products": load_products(), "empresa": EMPRESA_NOMBRE,
        "telf": EMPRESA_TELF, "iva_rate": IVA_RATE,
    })


@router.post("/cotizacion/{quote_id}/editar")
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
        if qty <= 0:
            continue
        items.append({
            "sku": (item_sku[i] or "").strip(),
            "name": name,
            "unit": (item_unit[i] or "unidad").strip(),
            "qty": qty,
            "unit_price": float(item_unit_price[i] or 0),
        })

    if not items:
        return RedirectResponse(url=f"/cotizacion/{quote_id}/editar?err=Agrega+al+menos+un+item", status_code=303)

    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            psql("UPDATE quotes SET client_name=?,delivery_time=?,validity_days=?,notes=? WHERE id=?"),
            (client_name.strip(), delivery_time.strip(), int(validity_days), notes.strip() or None, quote_id),
        )
        cur.execute(psql("DELETE FROM quote_items WHERE quote_id=?"), (quote_id,))
        for it in items:
            cur.execute(
                psql("INSERT INTO quote_items(quote_id,sku,name,unit,qty,unit_price) VALUES(?,?,?,?,?,?)"),
                (quote_id, it["sku"] or None, it["name"], it["unit"], it["qty"], it["unit_price"]),
            )
        con.commit()
    finally:
        con.close()

    return RedirectResponse(url="/historial", status_code=303)


@router.post("/cotizacion/{quote_id}/borrar")
def borrar(request: Request, quote_id: int):
    gate = require_login(request)
    if gate:
        return gate

    con = db_connect()
    try:
        cur = con.cursor()
        cur.execute(psql("DELETE FROM quote_items WHERE quote_id=?"), (quote_id,))
        cur.execute(psql("DELETE FROM quotes WHERE id=?"), (quote_id,))
        con.commit()
    finally:
        con.close()

    return RedirectResponse(url="/historial", status_code=303)
