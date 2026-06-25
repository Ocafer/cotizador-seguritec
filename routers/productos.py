from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth import require_login
from config import EMPRESA_NOMBRE
from database import db_exec, db_fetchone, psql
from services import load_all_products
from templating import templates

router = APIRouter()


# ---------------------------------------------------------------------------
# Catálogo de productos
# ---------------------------------------------------------------------------

@router.get("/productos", response_class=HTMLResponse)
def productos_get(request: Request):
    gate = require_login(request)
    if gate:
        return gate
    return templates.TemplateResponse("productos.html", {
        "request": request, "empresa": EMPRESA_NOMBRE,
        "productos": load_all_products(),
        "msg": request.query_params.get("msg", ""),
        "msg_type": request.query_params.get("msg_type", "success"),
    })


@router.post("/productos/guardar")
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
        db_exec(
            psql("UPDATE products SET sku=?,categoria=?,nombre=?,unidad=?,precio_bs=? WHERE id=?"),
            (sku, categoria, nombre, unidad, precio_bs, int(producto_id)),
        )
        msg = "Producto actualizado correctamente."
    else:
        db_exec(
            psql("INSERT INTO products(sku,categoria,nombre,unidad,precio_bs,activo) VALUES(?,?,?,?,?,1)"),
            (sku, categoria, nombre, unidad, precio_bs),
        )
        msg = "Producto agregado correctamente."

    return RedirectResponse(url=f"/productos?msg={msg}&msg_type=success", status_code=303)


@router.post("/productos/{producto_id}/toggle")
def productos_toggle(request: Request, producto_id: int):
    gate = require_login(request)
    if gate:
        return gate
    db_exec(psql("UPDATE products SET activo = CASE WHEN activo=1 THEN 0 ELSE 1 END WHERE id=?"), (producto_id,))
    return RedirectResponse(url="/productos?msg=Estado+actualizado.&msg_type=success", status_code=303)


@router.post("/productos/{producto_id}/borrar")
def productos_borrar(request: Request, producto_id: int):
    gate = require_login(request)
    if gate:
        return gate
    db_exec(psql("DELETE FROM products WHERE id=?"), (producto_id,))
    return RedirectResponse(url="/productos?msg=Producto+eliminado.&msg_type=warning", status_code=303)


# ---------------------------------------------------------------------------
# Carga masiva de productos
# ---------------------------------------------------------------------------

CAMARAS_WIFI = [
    ("CL1B-5-E27","IMOU - Smart","Bombilla Smart Wifi LED 2.4G - Control Grupo - Colores - Alexa/Google - 70M","unidad",111),
    ("IPC-C22FN-C-IMOU","IMOU - Cámaras Wifi","Cámara Wifi Versa 2MP Full Color - Lente 2.8mm - 114 - H.265 - Audio 2 Vías - Sirena - MicroSD 256GB - IP65","unidad",455),
    ("IPC-GK2CN-3C1WR-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Ranger RC 3MP - Rastreo Inteligente - Modo Privacidad - Llamada 1 Toque - Sirena - MicroSD 256GB - Audio Bidireccional","unidad",361),
    ("IPC-GS7EN-3M0WE-IMOU","IMOU - Cámaras Wifi","Cámara Cruiser 2 IP Wifi 2K - Detec. Personas/Vehículos - Full Color - IP66 - Audio Bidireccional - Rastreo - MicroSD 256GB - Lente 3.6mm","unidad",674),
    ("IPC-GS7EN-5M0WE-IMOU","IMOU - Cámaras Wifi","Cámara Cruiser IP Wifi 5MP Full Color - Detec. Humanos/Vehículos - Disuasión Activa - IP66 - Audio Bidireccional - MicroSD 256GB - Rastreo","unidad",728),
    ("IPC-K2EN-3H3W-IMOU","IMOU - Cámaras Wifi","Cámara Ranger 2 IP Wifi 3MP - Audio Bidireccional - Auto Crucero - Sirena - MicroSD 512GB - Rastreo Inteligente - ONVIF","unidad",312),
    ("IPC-K2EN-5H3W-IMOU","IMOU - Cámaras Wifi","Cámara Ranger 2 IP Wifi 5MP - Audio Bidireccional - Auto Crucero - Sirena - MicroSD 512GB - Rastreo Inteligente - ONVIF","unidad",440),
    ("IPC-K3DN-3H0WF-0280B-IMOU","IMOU - Cámaras Wifi","Cámara Bala Bullet 2E 3MP Full Color - Detec. Humanos - IP67 - Mic Integrado - MicroSD 512GB - RJ45 - Lente 2.8mm","unidad",369),
    ("IPC-K3DN-5H0WF-0280B-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Bala 5MP Bullet 2E Full Color - Plástica - Lente 2.8mm - Detec. Humanos - Mic Integrado - MicroSD 512GB - IP67","unidad",449),
    ("IPC-K7FN-3H0WE-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Cruiser SC 3MP - Tipo PT - Full Color - Rastreo Inteligente - Audio Bidireccional - RJ45 - MicroSD 512GB - Exterior - Disuasiva","unidad",465),
    ("IPC-K7FN-5H0WE-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Cruiser SC+ 5MP - Detec. Humanos - Rastreo Inteligente - Disuasión Activa - IP66 - Audio Bidireccional - SD 512GB - Full Color","unidad",517),
    ("IPC-K9DCN-3T0WE/FSP12","IMOU - Kits Solar","Kit IP Wifi Cell 3C Exterior 3MP - Batería Integrada - Sirena/Reflector - Audio Bidireccional - IP66 - MicroSD 256GB - Panel Solar","unidad",863),
    ("IPC-K9ECN-3T0WE/FSP12","IMOU - Kits Solar","Kit IP Wifi Cell PT Lite 3MP - Batería Integrada - Sirena/Reflector - Audio Bidireccional - IP66 - MicroSD 256GB - Panel Solar","unidad",1124),
    ("IPC-S2EN-3R1S-IMOU","IMOU - Cámaras Wifi","Cámara Ranger 2 IP Wifi 3MP - Llamada 1 Toque - Detec. Personas/Mascotas - Audio Bidireccional - Full Color - MicroSD 512GB","unidad",335),
    ("IPC-S2EN-5R1S-IMOU","IMOU - Cámaras Wifi","Cámara Ranger 2 IP Wifi 5MP - Llamada 1 Toque - Detec. Personas/Mascotas - Audio Bidireccional - Full Color - MicroSD 512GB","unidad",399),
    ("IPC-S3EN-3M0WE-0280B-IMOU","IMOU - Cámaras Wifi","Cámara IP Inalámbrica Bala 3MP Full Color - Detec. Humanos/Vehículos - IP67 - Audio Doble Vía - POE - MicroSD 256GB - Plástica/Metal","unidad",480),
    ("IPC-S3EN-5M0WE-0280B-IMOU","IMOU - Cámaras Wifi","Cámara Wifi Bullet 3 5MP Full Color - Detec. Humanos/Vehículos - IP67 - Audio Doble Vía - MicroSD 256GB - Plástica/Metal","unidad",567),
    ("IPC-S2XN-6M0WED-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Ranger Dual 6MP - Doble Lente - Detec. IA Personas/Vehículos - Disuasión Activa - Audio Bidireccional - Full Color - MicroSD 256GB","unidad",515),
    ("IPC-S2XEN-6M0S-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Ranger Dual 3MP+3MP - Botón Llamada - Detec. Personas/Mascotas - Rastreo Inteligente - Audio Bidireccional - MicroSD 512GB","unidad",545),
    ("IPC-S6DN-3M0WEB-E27-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Tipo Foco 3MP - PTZ - Disuasión Activa - Detec. IA Vehículos/Personas - Full Color - Audio Bidireccional - MicroSD 256GB - ONVIF","unidad",380),
    ("IPC-T26EN-0280B-IMOU","IMOU - Cámaras Wifi","Cámara IP Turret Wifi 2MP - Lente 2.8mm - IR 30M - H.265/H.264 - Audio Doble Vía - Sirena/Reflector - ONVIF - MicroSD 256GB - IP67","unidad",597),
    ("IPC-S7XEN-8M0WED-0360B","IMOU - Cámaras Wifi","Cámara Cruiser Dual 2 IP Wifi 3+5MP - Tipo PT - Luces Disuasivas Rojo/Azul - Sirena - Full Color - Audio Bidireccional","unidad",699),
    ("IPC-S7UN-11M0WED-IMOU","IMOU - Cámaras Wifi","Cámara IP Wifi Cruiser Triple 3+3+5MP - 2 Ranuras MicroSD 1024GB - Audio Bidireccional - Visión Nocturna 30M - Exterior","unidad",961),
    ("DH-IPC-C3AP-0280B","Dahua - Cámaras Wifi","Cámara IP Cubo Wifi 3MP - Interior - Detec. Humanos IA - Detec. Mascotas - Audio Bidireccional - Wifi 6 - Bluetooth - MicroSD 256GB","unidad",283),
    ("DH-IPC-C5AP-0280B","Dahua - Cámaras Wifi","Cámara IP Wifi Cube C1 5MP - Interior - Detec. Humanos IA - Detec. Mascotas - Audio Bidireccional - Wifi 6 - Bluetooth - MicroSD 256GB","unidad",335),
    ("DH-IPC-H3AP-0360B","Dahua - Cámaras Wifi","Cámara IP Hero A1 Wifi 3MP Tipo PT - 360 - Audio Bidireccional - Detec. IA Humanos - Detec. Mascotas - Wifi 6 - Auto Tracking - MicroSD 256GB","unidad",357),
    ("DH-IPC-H5AP-0360B","Dahua - Cámaras Wifi","Cámara IP Hero 1 Wifi 5MP Tipo PT - 360 - Audio Bidireccional - Detec. IA Humanos - Detec. Mascotas - Wifi 6 - Auto Tracking - MicroSD 256GB","unidad",389),
    ("DH-IPC-H3BP-0360B","Dahua - Cámaras Wifi","Cámara IP Wifi 3MP - Llamada 1 Toque - Rotación 360 - Audio Bidireccional - Detec. IA Humanos - Detec. Mascotas - Rastreo Inteligente","unidad",364),
    ("DH-IPC-H5BP-0360B","Dahua - Cámaras Wifi","Cámara Hero B1 IP Wifi 5MP - Rotación 360 - Llamada 1 Toque - Audio Bidireccional - Detec. IA Humanos - Detec. Mascotas - Pareo Bluetooth","unidad",424),
    ("DH-SD-H4C-0400B","Dahua - Cámaras Wifi","Cámara IP Wifi 4MP Tipo PT - Sensor CMOS - 25/30fps@1080p - Detec. Personas - Audio Bidireccional - Alarma Integrada - RJ45","unidad",494),
    ("DH-IPC-H3DP-3F-0360B","Dahua - Cámaras Wifi","Cámara Wifi Tipo PT Doble Lente 3+3MP - Ángulos Ajustables - Llamada 1 Toque - Detec. Personas/Mascotas - Auto Tracking - MicroSD 256GB","unidad",520),
    ("DH-IPC-H5DP-5F-0360B","Dahua - Cámaras Wifi","Cámara Wifi Tipo PT Doble Lente 5+5MP - Ángulos Ajustables - Llamada 1 Toque - Detec. Personas/Mascotas - Auto Tracking - MicroSD 256GB","unidad",604),
    ("DH-IPC-P3ASP-PV-0400B-S2","Dahua - Cámaras Wifi","Cámara IP Wifi Picoo 3MP - Lente 4mm - Alarma Sonido/Luz - Detec. IA Personas/Vehículos - IP66 - Modo Patrullaje - Audio Bidireccional - MicroSD 256GB","unidad",515),
    ("DH-IPC-P5ASP-PV-0400B-S2","Dahua - Cámaras Wifi","Cámara IP Wifi 5MP Tipo PT - Detec. IA Personas/Vehículos - Audio Bidireccional - Auto Tracking - IP66 - Alarma Sonido/Luz - MicroSD 256GB","unidad",602),
    ("DH-IPC-P3BP-PV-0360B","Dahua - Cámaras Wifi","Cámara IP Picoo B1 Wifi 3MP Tipo PT - Smart Dual Light - 360 - Alarma Sonido/Luz - Detec. IA Personas/Vehículos - Wifi 6 - MicroSD 256GB","unidad",499),
    ("DH-IPC-P5BP-PV-0360B","Dahua - Cámaras Wifi","Cámara IP Wifi Picoo B1 5MP TIOC - Smart Dual Light - 360 - Detec. Humanos/Vehículos IA - Auto Tracking - Wifi 6 - IP66 - MicroSD 256GB","unidad",620),
    ("DH-IPC-P5FP-PV-0360B-PRO","Dahua - Cámaras Wifi","Cámara IP Wifi 5MP WizColor PT - Rastreo Inteligente - Detec. IA Personas/Vehículos - IP66 - Audio Bidireccional - Alarma Sonido/Luz - MicroSD 512GB","unidad",703),
    ("IPC-P3DP-3F-PV-0280B","Dahua - Cámaras Wifi","Cámara IP Wifi 3x3MP Doble Lente Tipo PT - Detección Dual - Control de Ronda - Sirena/Luz - Detec. IA Personas/Vehículos - IP66 - MicroSD 256GB","unidad",772),
    ("DH-IPC-P3DP-3F-PV-P-PRO","Dahua - Cámaras Wifi","Cámara IP Tipo PT Wifi Doble Lente 3+3MP WizColor - Audio Bidireccional - Wifi 6 - Detec. Personas/Vehículos - IP66 - MicroSD 256GB - Rastreo","unidad",1050),
    ("IPC-P5DP-5F-PV-0280B","Dahua - Cámaras Wifi","Cámara IP Wifi 5x5MP Doble Lente Tipo PT - Detección Dual - Control de Ronda - Sirena/Luz - Detec. IA Personas/Vehículos - IP66 - MicroSD 256GB","unidad",865),
    ("DH-IPC-F2CN-PV-0280B","Dahua - Cámaras Wifi","Cámara IP Wifi 2MP Tipo Bala - Detec. Humanos IA - Smart Dual Light - Disuasión Activa - Audio Bidireccional - IP67 - MicroSD 256GB","unidad",655),
    ("DH-F3D-PV","Dahua - Cámaras Wifi","Cámara Wifi Tipo Bala 3MP - IP67 - Detec. Humanos IA - MicroSD 256GB - Smart Dual Light - Disuasión Activa - Audio Bidireccional","unidad",450),
    ("DH-IPC-T2AN-PV-0280B","Dahua - Cámaras Wifi","Cámara IP Wifi Domo 2MP - Lente 2.8mm - Detec. Humanos AI - Disuasión Activa - Audio Bidireccional - IP67 - MicroSD 256GB","unidad",630),
    ("DH-P4F-PV-4G","Dahua - Cámaras 4G","Cámara Tipo PT 4MP 4G WizColor - 360 - MicroSD 256GB - Audio Bidireccional","unidad",1092),
    ("DH-IPC-HFW2441DG-4G-SP-B","Dahua - Cámaras 4G","Cámara IP Bala 4G 4MP - Panel Solar - Batería - SMD - Sirena/Luz Disuasión - Audio Bidireccional - IR 50M - Luz Cálida 30M - MicroSD - IP67","unidad",3638),
    ("DMS-BAT-L1+SOL-SP5","Smart Wifi - Exterior","Cámara Smart Exterior con Batería Recargable 6 meses - 2MP - Soporta Panel Solar DMS-SOL-SP5 - Requiere MicroSD","unidad",466),
    ("DMS-REF-D7","Smart Wifi - Exterior","Cámara Smart Exterior con Reflectores LED 2500 Lúmenes - 3MP - Compatible App Smart Life y Tuya - Requiere MicroSD","unidad",901),
    ("DMS-LB002-E26-W","Smart Wifi - Accesorios","Socket Smart para automatizar luces tipo bombillos E26","unidad",123),
    ("G103","Smart Wifi - Interior","Cámara Cubo Smart 3MP - Interior - Detec. Movimiento - Audio Bidireccional - 100 grados - MicroSD 128GB - IR 10M","unidad",149),
    ("G104","Smart Wifi - Interior","Cámara Smart PTZ 3MP - Interior - Detec. Movimiento - Audio Bidireccional - MicroSD 128GB - Visión Nocturna - Wifi 2.4GHz","unidad",189),
    ("G105","Smart Wifi - Interior","Cámara Socket PTZ 4MP - Interior - Visión Nocturna - Detec. Movimiento - Audio Bidireccional - MicroSD 128GB - Puerto E27","unidad",218),
    ("G106","Smart Wifi - Interior","Cámara Panorámica 3MP FHD - Interior - 360 grados - MicroSD 128GB - Visión Nocturna - Audio Bidireccional - Diámetro 7cm","unidad",253),
    ("G116","Smart Wifi - Interior","Cámara IP Wifi 3MP - Llamada 1 Toque - Lente 3.6mm - Visión Nocturna 10M - MicroSD 256GB - Rotación 355 grados - Rastreo - Audio Bidireccional","unidad",215),
    ("G117","Smart Wifi - Interior","Cámara para Bebé IP Wifi 3MP - Detec. Personas/Llanto - Canciones de Cuna - Recordatorio Alimentación - Audio Bidireccional - MicroSD 256GB","unidad",216),
    ("G118","Smart Wifi - Exterior","Cámara PT 4G con Panel Solar y Batería - 3MP - Full Color - Sensor PIR - Audio Bidireccional - Visión Nocturna 15M - IP65 - MicroSD 256GB","unidad",870),
    ("G119","Smart Wifi - Exterior","Cámara PT Wifi 4MP con Panel Solar y Batería - Full Color - Sensor PIR - Audio Bidireccional - Visión Nocturna 15M - IP65 - MicroSD 256GB","unidad",860),
    ("G120","Smart Wifi - Exterior","Cámara PT Wifi 4MP Tipo Foco - Detec. Personas/Movimiento - Audio Bidireccional - IR 8-10M - Wifi 2.4GHz - MicroSD 256GB","unidad",235),
    ("G121","Smart Wifi - Exterior","Cámara Wifi Fija 3MP Full Color - Detec. Personas/Movimiento - Ilum. 8M - Audio Bidireccional - Wifi 2.4GHz - MicroSD 256GB","unidad",207),
    ("G122","Smart Wifi - Exterior","Cámara PT Wifi 4MP Exterior - Rastreo Movimiento - Detec. Personas - Full Color - IP65 - Audio Bidireccional - Ilum. 10M - MicroSD 256GB","unidad",335),
    ("G123","Smart Wifi - Accesorios","Kit Timbre Inalámbrico 4MP - Batería 5200mAh - Sensor PIR 9M - IP64 - Audio Bidireccional - Alarma Antidesmontaje - Duración 2-3 meses","unidad",450),
    ("TAPO-C200","Tapo - Cámaras Wifi","Cámara Wifi Tapo C200 - Rotación 360 - Full HD 1080P - Audio 2 Vías - Visión Nocturna - Detec. Movimiento - Modo Privacidad - MicroSD 128GB","unidad",246),
    ("TAPO-C210","Tapo - Cámaras Wifi","Cámara Wifi Tapo C210 - Rotación 360 - 2K 3MP - Audio Bidireccional - Visión Nocturna - Detec. Movimiento - MicroSD 512GB","unidad",262),
    ("TAPO-C220","Tapo - Cámaras Wifi","Cámara Wifi Tapo C220 - 2K QHD 4MP - Detec. IA Inteligente - Detec. Llanto Bebé - Visión Nocturna 10M - Interior - Detec. Mascota","unidad",330),
    ("TAPO-C225","Tapo - Cámaras Wifi","Cámara Wifi Tapo C225 - 2K QHD 4MP - Detec. IA - Modo Privado Manual - Alarma Luz/Sonido - Visión Nocturna - Interior","unidad",505),
    ("TAPO-C310","Tapo - Cámaras Wifi","Cámara Wifi Outdoor Tapo C310 - 3MP - Audio Bidireccional - Visión Nocturna - Detec. Movimiento - MicroSD 128GB - IP66","unidad",427),
    ("TAPO-C320WS","Tapo - Cámaras Wifi","Cámara Wifi Outdoor Tapo C320WS - 2K QHD - Audio Bidireccional - Visión Nocturna - Detec. Movimiento - MicroSD 256GB - IP66","unidad",477),
    ("TAPO-C500","Tapo - Cámaras Wifi","Cámara Wifi Outdoor Tapo C500 - 2MP - Audio Bidireccional - Visión Nocturna - Detec. Movimiento - MicroSD 512GB - IP65","unidad",401),
    ("TAPO-C510W","Tapo - Cámaras Wifi","Cámara Wifi Outdoor Tapo C510W - 3MP - Audio Bidireccional - Visión Nocturna - Detec. Movimiento - MicroSD 512GB - IP65","unidad",425),
    ("TAPO-L510E","Tapo - Smart","Bombilla Smart Wifi Regulable Tapo L510E - Control Iluminación - Agenda/Temporizador - Control de Voz - Modo Ausente - 8.7W - 806 Lúmenes - Alexa","unidad",87),
    ("TAPO-P105","Tapo - Smart","Mini Enchufe Smart Wifi Tapo P105 1-Pack - Wifi 2.4GHz/BT 4.2 - Temporizador - Modo Ausente - Alexa y Google Assistant","unidad",97),
    ("TAPO-P100","Tapo - Smart","Mini Enchufe Smart Wifi Tapo P100 1-Pack - Wifi 2.4GHz/BT 4.2 - Temporizador - Modo Ausente - Alexa y Google Assistant","unidad",109),
]

PRODUCTOS_VARIOS = [
    ("Z050","Networking - Switches","Switch POE 4 Puertos - 2 Uplink 10/100M - IEEE 802.3/802.3u/802.3af/a - Dist. TX POE 250M con UTP Cat6 - Dimensiones 200x120x45mm","unidad",215.00),
    ("DH-CS4218-18ET-135","Networking - Switches","Switch Administrable 18 Puertos Metálico - 16P POE Potencia 90W - Dist. Max UTP 250M - POE Watchdog - 16P RJ45 10/100Mbps - 2P RJ45 10/100/1000Mbps - 2P SFP","unidad",1744.00),
    ("DHI-ASA1222GL-D","Control de Asistencia","Control Asistencia Standalone - Pantalla 2.4 - 1000 Usuarios - 2000 Huellas - 1000 Passwords - 1000 Tarjetas - Importa/Exporta datos USB Flash","unidad",685.00),
    ("DHI-ASA1222EL-S","Control de Asistencia","Control Asistencia Standalone - Pantalla 2.4 - 1000 Usuarios - 2000 Huellas - 1000 Passwords - Importa/Exporta datos USB Flash - 1 RJ45","unidad",441.00),
    ("SenseFP-M1","Control de Asistencia","Control Asistencia - Pantalla 2.8 - Huella 500 - RFID 500 125kHz - Cap. Reg. 100000 - USB Host - Wifi - Software ZLink - Incluye Fuente","unidad",692.00),
    ("M1-FP-RFID","Control de Asistencia","Control de Asistencia - Pantalla 2.8 - Teclado Físico - RFID 1000 - Huella 1000 - Wifi - Software BioTime Cloud/ZKBio CVAccess - Incluye Fuente de Poder","unidad",585.00),
    ("F302","Accesorios - Baluns","Balun Pasivo 4 en 1 - Transmisor Video HDCVI/AHD/Análogo/TVI - Distancia Max 440M en 720P - Reducción de Ruido","unidad",15.00),
    ("Z043","Accesorios - Cajas","Caja de Conexiones para Cámaras Cuadrada - Material ABS - Temperatura -10 a 60C - IP44 - Dimensiones 11.8x10x5cm","unidad",12.00),
    ("C723","Cableado - UTP","Cable UTP Categoría 6 CCAE 80% Cobre - 4 Pares - 2x0.57mm 23AWG 75 grados - Presentación 305 Metros - Color Blanco - Bajo Norma UL","rollo",604.00),
    ("C712","Cableado - Conectores","Conectores RJ45 Categoría 6 - Diámetro Inserción para Cable 7.8mm - Paquete de 50 Unidades","paquete",16.00),
    ("HUA722020ALA331","Almacenamiento - Discos","Disco Duro New Pull 2TB Ultrastar Hitachi - Cache 32MB - 7200RPM - SATA 3.0Gb/s - Enterprise","unidad",423.00),
    ("SE-R9U-5409A","Infraestructura - Gabinetes","Gabinete 9U 19 Negro - Puerta de Vidrio","unidad",600.00),
    ("IPC-HDW1230T1P-0280B-S6","Cámaras IP - Domo","Cámara IP Domo 2MP Metal/Plástica - Lente 2.8mm - Sensor 1/2.8 CMOS - IR 30M - ROI - Smart H.265+/H.264+ - DWDR - 3D NR - IP67 - 12V/PoE","unidad",385.00),
    ("DHI-NVR2116HS-4KS3","Grabadores - NVR","Grabador IP 16 Canales - Protec. Perimetral - SMD Plus - 1VGA - 1HDMI - 1HDD hasta 16TB - 1RJ45 - ONVIF - Resolución 12MP/8MP/5MP/4MP/3MP/2MP/720p","unidad",745.00),
]


def _bulk_insert(dataset, label: str, count: int):
    insertados = 0
    omitidos = 0
    for sku, categoria, nombre, unidad, precio in dataset:
        existe = db_fetchone(psql("SELECT id FROM products WHERE sku=?"), (sku,))
        if not existe:
            db_exec(
                psql("INSERT INTO products(sku,categoria,nombre,unidad,precio_bs,activo) VALUES(?,?,?,?,?,1)"),
                (sku, categoria, nombre, unidad, precio),
            )
            insertados += 1
        else:
            omitidos += 1
    return insertados, omitidos


@router.get("/admin/cargar-camaras-wifi", response_class=HTMLResponse)
def cargar_camaras_wifi(request: Request, confirmar: str = ""):
    gate = require_login(request)
    if gate:
        return gate
    if confirmar != "si":
        return HTMLResponse("""
        <html><body style="font-family:sans-serif;padding:30px;max-width:500px;margin:auto;">
        <h3>Carga masiva: Cámaras Wifi Enero 2026</h3>
        <p>Se cargarán <strong>68 productos</strong> (IMOU, Dahua, Smart Wifi, Tapo).</p>
        <p>Los productos con SKU duplicado serán <strong>omitidos</strong> automáticamente.</p>
        <a href="/admin/cargar-camaras-wifi?confirmar=si"
           style="background:#198754;color:white;padding:12px 24px;border-radius:6px;text-decoration:none;display:inline-block;margin-top:10px;">
           Confirmar y cargar productos
        </a>&nbsp;
        <a href="/productos" style="color:#666;">Cancelar</a>
        </body></html>""")
    ins, om = _bulk_insert(CAMARAS_WIFI, "Cámaras Wifi", 68)
    return HTMLResponse(f"""
    <html><body style="font-family:sans-serif;padding:30px;max-width:500px;margin:auto;">
    <h3>Carga completada</h3>
    <p><strong>{ins}</strong> productos insertados correctamente.</p>
    <p><strong>{om}</strong> productos omitidos (SKU ya existía).</p>
    <a href="/productos" style="background:#0d6efd;color:white;padding:12px 24px;border-radius:6px;text-decoration:none;display:inline-block;margin-top:10px;">Ver productos</a>
    </body></html>""")


@router.get("/admin/cargar-productos-varios", response_class=HTMLResponse)
def cargar_productos_varios(request: Request, confirmar: str = ""):
    gate = require_login(request)
    if gate:
        return gate
    if confirmar != "si":
        return HTMLResponse("""
        <html><body style="font-family:sans-serif;padding:30px;max-width:500px;margin:auto;">
        <h3>Carga masiva: Productos Varios (Feb/Mar 2026)</h3>
        <p>Se cargarán <strong>14 productos</strong>: Switches POE, Controles de Asistencia, Accesorios, Cableado, Almacenamiento, Grabadores.</p>
        <p>Los productos con SKU duplicado serán <strong>omitidos</strong> automáticamente.</p>
        <a href="/admin/cargar-productos-varios?confirmar=si"
           style="background:#198754;color:white;padding:12px 24px;border-radius:6px;text-decoration:none;display:inline-block;margin-top:10px;">
           Confirmar y cargar productos
        </a>&nbsp;
        <a href="/productos" style="color:#666;">Cancelar</a>
        </body></html>""")
    ins, om = _bulk_insert(PRODUCTOS_VARIOS, "Productos Varios", 14)
    return HTMLResponse(f"""
    <html><body style="font-family:sans-serif;padding:30px;max-width:500px;margin:auto;">
    <h3>Carga completada</h3>
    <p><strong>{ins}</strong> productos insertados correctamente.</p>
    <p><strong>{om}</strong> productos omitidos (SKU ya existía).</p>
    <a href="/productos" style="background:#0d6efd;color:white;padding:12px 24px;border-radius:6px;text-decoration:none;display:inline-block;margin-top:10px;">Ver productos</a>
    </body></html>""")
