from __future__ import annotations
import io
import os
from typing import List, Optional

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

from config import EMPRESA_NOMBRE, EMPRESA_TELF, IVA_RATE, LOGO_PATH


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
    c.drawString(x0, y, EMPRESA_NOMBRE)
    y -= 7 * mm
    c.setFont("Helvetica", 10)
    c.drawString(x0, y, f"Telf.: {EMPRESA_TELF}")
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
