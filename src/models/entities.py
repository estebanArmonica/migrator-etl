from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Barra:
    id_barra: int
    nombre: str
    created_at: datetime

@dataclass
class DimTiempo:
    id_tiempo: int
    fecha: datetime
    hora: int
    minuto: int
    cuarto_hora: int
    clave_anio_mes: str
    created_at: datetime

@dataclass
class Empresa:
    id_emp: int
    nombre: str
    tipo: Optional[str]
    created_at: datetime

@dataclass
class PrecioMarginal:
    id_pr_mrgl: int
    tiempo_id: int
    barra_id: int
    cmg_mills_kwh: float
    cmg_usd_kwh: float
    usd: float
    created_at: datetime

@dataclass
class RetiroEnergia:
    id_rt: int
    tiempo_id: int
    barra_id: int
    suministrador_id: int
    retiro_id: int
    clave: str
    tipo: str
    medida_kwh: float
    created_at: datetime

@dataclass
class ContratoFisico:
    id_ct: int
    tiempo_id: int
    barra_id: int
    clave: str
    empresa_id: int
    transaccion: str
    kwh: float
    valorizado_clp: float
    id_contrato: int
    cmg_peso_kwh: float
    created_at: datetime

@dataclass
class TipoTransaccion:
    id_tip_trans: int
    nombre: str
    descripcion: Optional[str]
    created_at: datetime