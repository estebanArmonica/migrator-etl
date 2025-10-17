import logging
from typing import List, Dict, Any
from src.config.database import DatabaseConnection

class BarraRepository:
    """Repositorio para operaciones de la tabla barra"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def insert_or_get_barras(self, barras_names: List[str]) -> Dict[str, int]:
        """Inserta barras nuevas y retorna mapeo nombre -> id"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Obtener barras existentes
                cursor.execute("SELECT id_barra, nombre FROM barra WHERE nombre = ANY(%s)", (barras_names,))
                existing_barras = {row['nombre']: row['id_barra'] for row in cursor.fetchall()}
                
                # Insertar nuevas barras
                new_barras = set(barras_names) - set(existing_barras.keys())
                barras_map = existing_barras.copy()
                
                for barra_name in new_barras:
                    cursor.execute(
                        "INSERT INTO barra (nombre) VALUES (%s) RETURNING id_barra",
                        (barra_name,)
                    )
                    barra_id = cursor.fetchone()['id_barra']
                    barras_map[barra_name] = barra_id
                    self.logger.info(f"Barra insertada: {barra_name} (ID: {barra_id})")
                
                conn.commit()
                return barras_map

class EmpresaRepository:
    """Repositorio para operaciones de la tabla empresa"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def insert_or_get_empresas(self, empresas_names: List[str]) -> Dict[str, int]:
        """Inserta empresas nuevas y retorna mapeo nombre -> id"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Obtener empresas existentes
                cursor.execute("SELECT id_emp, nombre FROM empresa WHERE nombre = ANY(%s)", (empresas_names,))
                existing_empresas = {row['nombre']: row['id_emp'] for row in cursor.fetchall()}
                
                # Insertar nuevas empresas
                new_empresas = set(empresas_names) - set(existing_empresas.keys())
                empresas_map = existing_empresas.copy()
                
                for empresa_name in new_empresas:
                    cursor.execute(
                        "INSERT INTO empresa (nombre, tipo) VALUES (%s, %s) RETURNING id_emp",
                        (empresa_name, 'Por Definir')
                    )
                    empresa_id = cursor.fetchone()['id_emp']
                    empresas_map[empresa_name] = empresa_id
                    self.logger.info(f"Empresa insertada: {empresa_name} (ID: {empresa_id})")
                
                conn.commit()
                return empresas_map

class TiempoRepository:
    """Repositorio para operaciones de la tabla dim_tiempo"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def insert_or_get_tiempos(self, tiempos_data: List[Dict[str, Any]]) -> Dict[tuple, int]:
        """Inserta tiempos nuevos y retorna mapeo (fecha, hora, minuto) -> id"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Crear tuplas para búsqueda
                tiempo_tuples = [(data['fecha'], data['hora'], data['minuto']) for data in tiempos_data]
                
                # Obtener tiempos existentes
                cursor.execute("""
                    SELECT id_tiempo, fecha, hora, minuto 
                    FROM dim_tiempo 
                    WHERE (fecha, hora, minuto) IN (%s)
                """, (tuple(tiempo_tuples),))
                
                existing_tiempos = {(row['fecha'], row['hora'], row['minuto']): row['id_tiempo'] 
                                  for row in cursor.fetchall()}
                
                # Insertar nuevos tiempos
                tiempos_map = existing_tiempos.copy()
                new_tiempos = [data for data in tiempos_data 
                             if (data['fecha'], data['hora'], data['minuto']) not in existing_tiempos]
                
                for tiempo in new_tiempos:
                    cursor.execute("""
                        INSERT INTO dim_tiempo (fecha, hora, minuto, cuarto_hora, clave_anio_mes)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id_tiempo
                    """, (
                        tiempo['fecha'], tiempo['hora'], tiempo['minuto'],
                        tiempo.get('cuarto_hora', (tiempo['hora'] * 4) + (tiempo['minuto'] // 15)),
                        tiempo.get('clave_anio_mes', tiempo['fecha'].strftime('%Y-%m'))
                    ))
                    
                    tiempo_id = cursor.fetchone()['id_tiempo']
                    key = (tiempo['fecha'], tiempo['hora'], tiempo['minuto'])
                    tiempos_map[key] = tiempo_id
                    self.logger.debug(f"Tiempo insertado: {key} (ID: {tiempo_id})")
                
                conn.commit()
                return tiempos_map

class DataRepository:
    """Repositorio principal para inserción de datos"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.barra_repo = BarraRepository(db_connection)
        self.empresa_repo = EmpresaRepository(db_connection)
        self.tiempo_repo = TiempoRepository(db_connection)
        self.logger = logging.getLogger(__name__)
    
    def insert_precios_marginales(self, precios_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de precios marginales"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO precio_marginal (tiempo_id, barra_id, cmg_mills_kwh, cmg_usd_kwh, usd)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, [
                    (precio['tiempo_id'], precio['barra_id'], precio['cmg_mills_kwh'], 
                     precio['cmg_usd_kwh'], precio['usd'])
                    for precio in precios_data
                ])
                
                count = len(precios_data)
                conn.commit()
                self.logger.info(f"Insertados {count} registros en precio_marginal")
                return count
    
    def insert_retiros_energia(self, retiros_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de retiros de energía"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO retiro_energia (tiempo_id, barra_id, suministrador_id, retiro_id, clave, tipo, medida_kwh)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, [
                    (retiro['tiempo_id'], retiro['barra_id'], retiro['suministrador_id'],
                     retiro['retiro_id'], retiro['clave'], retiro['tipo'], retiro['medida_kwh'])
                    for retiro in retiros_data
                ])
                
                count = len(retiros_data)
                conn.commit()
                self.logger.info(f"Insertados {count} registros en retiro_energia")
                return count
    
    def insert_contratos_fisicos(self, contratos_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de contratos físicos"""
        with self.db.get_connection() as conn:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO contrato_fisico (tiempo_id, barra_id, clave, empresa_id, transaccion, kwh, valorizado_clp, id_contrato, cmg_peso_kwh)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, [
                    (contrato['tiempo_id'], contrato['barra_id'], contrato['clave'],
                     contrato['empresa_id'], contrato['transaccion'], contrato['kwh'],
                     contrato['valorizado_clp'], contrato['id_contrato'], contrato['cmg_peso_kwh'])
                    for contrato in contratos_data
                ])
                
                count = len(contratos_data)
                conn.commit()
                self.logger.info(f"Insertados {count} registros en contrato_fisico")
                return count