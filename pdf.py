from __future__ import annotations
import io
import os
from typing import List, Optional

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from config import EMPRESA_NOMBRE as _DEF_EMPRESA, EMPRESA_TELF as _DEF_TELF, IVA_RATE as _DEF_IVA, LOGO_PATH
from settings import get_setting


def money(x: float, simbolo: str = "Bs") -> str:
    return f"{simbolo} {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def wrap_text(c, text: str, max_width: float, font_name: str = "Helvetica", font_size: float = 10) -> list:
    words = text.split()
    lines, current = [], ""
    for word in words:
        test = (current + " " + word).strip()
        if c.stringWidth(test, font_name, font_size) <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines or [text]


def generate_pdf(
    quote_no: int,
    created_at: str,
    client_name: str,
    delivery_time: str,
    validity_days: int,
    items: List[dict],
    notes: Optional[str] = None,
) -> bytes:
    empresa = get_setting("empresa_nombre", _DEF_EMPRESA)
    telf = get_setting("empresa_telf", _DEF_TELF)
    iva_rate = float(get_setting("iva_rate", str(_DEF_IVA)))
    moneda = get_setting("moneda_simbolo", "Bs")

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    x0 = 18 * mm
    y = height - 18 * mm

    logo_w = 28 * mm
    logo_h = 28 * mm
    logo_x = width - x0 - logo_w
    logo_y = y - logo_h + 4 * mm

    if LOGO_PATH and os.path.exists(LOGO_PATH):
        try:
            c.drawImage(LOGO_PATH, logo_x, logo_y, width=logo_w, height=logo_h,
                        mask="auto", preserveAspectRatio=True)
        except Exception:
            pass

    c.setFont("Helvetica-Bold", 15)
    c.drawString(x0, y, empresa)
    y -= 7 * mm
    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"Telf.: {telf}")
    y -= 7 * mm

    c.setLineWidth(0.8)
    c.line(x0, y, width - x0, y)
    y -= 8 * mm

    c.setFont("Helvetica-Bold", 13)
    c.drawString(x0, y, "COTIZACION")
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

        label = f"{name} ({unit})"
        col_w = 88 * mm
        text_lines = wrap_text(c, label, col_w)
        for idx, ln in enumerate(text_lines):
            c.drawString(x0, y, ln)
            if idx == 0:
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

    iva = subtotal * iva_rate
    total = subtotal + iva

    c.setFont("Helvetica-Bold", 10)
    c.drawRightString(x0 + 140 * mm, y, "Subtotal (Sin IVA):")
    c.drawRightString(width - x0, y, money(subtotal, moneda))
    y -= 6 * mm

    c.drawRightString(x0 + 140 * mm, y, f"IVA ({int(iva_rate * 100)}%):")
    c.drawRightString(width - x0, y, money(iva, moneda))
    y -= 6 * mm

    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(x0 + 140 * mm, y, "Total (Con IVA):")
    c.drawRightString(width - x0, y, money(total, moneda))
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
    c.drawString(x0, 12 * mm, f"{empresa} - Cotización generada automáticamente")
    c.showPage()
    c.save()
    return buf.getvalue()
