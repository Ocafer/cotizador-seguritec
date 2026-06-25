from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Product:
    sku: str
    categoria: str
    nombre: str
    unidad: str
    precio_bs: float
    activo: int = 1


@dataclass
class ProductoRow:
    id: int
    sku: str
    categoria: str
    nombre: str
    unidad: str
    precio_bs: float
    activo: int


@dataclass
class TecnicoRow:
    id: int
    nombre: str
    telefono: str
    especialidad: str
    activo: int
    total_instalaciones: int = 0


@dataclass
class ClienteRow:
    id: int
    nombre: str
    telefono: str
    direccion: str
    ci_nit: str
    tipo: str
    notas: str


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


@dataclass
class GastoRow:
    id: int
    quote_id: int
    categoria: str
    descripcion: str
    monto: float


@dataclass
class InstalacionManualRow:
    id: int
    fecha_instalacion: str
    cliente_nombre: str
    tecnicos: str
    descripcion: str
    monto_cobrado: float
    gastos: float
    notas: str

    @property
    def utilidad(self):
        return self.monto_cobrado - self.gastos

    @property
    def margen(self):
        return (self.utilidad / self.monto_cobrado * 100) if self.monto_cobrado > 0 else 0
