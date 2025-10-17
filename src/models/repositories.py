import logging
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

class BarraRepository:
    """Repositorio para operaciones de la tabla barra"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def insert_or_get_barras(self, barras_names: List[str]) -> Dict[str, int]:
        """Inserta barras nuevas y retorna mapeo nombre -> id"""
        session = self.db.get_session()
        try:
            # Obtener barras existentes
            placeholders = ', '.join([':name' + str(i) for i in range(len(barras_names))])
            params = {'name' + str(i): name for i, name in enumerate(barras_names)}
            
            query = f"SELECT id_barra, nombre FROM barra WHERE nombre IN ({placeholders})"
            result = session.execute(text(query), params)
            existing_barras = {row['nombre']: row['id_barra'] for row in result}
            
            # Insertar nuevas barras
            new_barras = set(barras_names) - set(existing_barras.keys())
            barras_map = existing_barras.copy()
            
            for barra_name in new_barras:
                result = session.execute(
                    text("INSERT INTO barra (nombre) VALUES (:nombre) RETURNING id_barra"),
                    {'nombre': barra_name}
                )
                barra_id = result.scalar()
                barras_map[barra_name] = barra_id
                self.logger.info(f"Barra insertada: {barra_name} (ID: {barra_id})")
            
            session.commit()
            return barras_map
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error en insert_or_get_barras: {e}")
            raise
        finally:
            session.close()

class EmpresaRepository:
    """Repositorio para operaciones de la tabla empresa"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def insert_or_get_empresas(self, empresas_names: List[str]) -> Dict[str, int]:
        """Inserta empresas nuevas y retorna mapeo nombre -> id"""
        session = self.db.get_session()
        try:
            # Obtener empresas existentes
            placeholders = ', '.join([':name' + str(i) for i in range(len(empresas_names))])
            params = {'name' + str(i): name for i, name in enumerate(empresas_names)}
            
            query = f"SELECT id_emp, nombre FROM empresa WHERE nombre IN ({placeholders})"
            result = session.execute(text(query), params)
            existing_empresas = {row['nombre']: row['id_emp'] for row in result}
            
            # Insertar nuevas empresas
            new_empresas = set(empresas_names) - set(existing_empresas.keys())
            empresas_map = existing_empresas.copy()
            
            for empresa_name in new_empresas:
                result = session.execute(
                    text("INSERT INTO empresa (nombre, tipo) VALUES (:nombre, :tipo) RETURNING id_emp"),
                    {'nombre': empresa_name, 'tipo': 'Por Definir'}
                )
                empresa_id = result.scalar()
                empresas_map[empresa_name] = empresa_id
                self.logger.info(f"Empresa insertada: {empresa_name} (ID: {empresa_id})")
            
            session.commit()
            return empresas_map
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error en insert_or_get_empresas: {e}")
            raise
        finally:
            session.close()

class TiempoRepository:
    """Repositorio para operaciones de la tabla dim_tiempo"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
    
    def insert_or_get_tiempos(self, tiempos_data: List[Dict[str, Any]]) -> Dict[tuple, int]:
        """Inserta tiempos nuevos y retorna mapeo (fecha, hora, minuto) -> id"""
        session = self.db.get_session()
        try:
            # Crear tuplas para búsqueda
            tiempo_tuples = [(data['fecha'], data['hora'], data['minuto']) for data in tiempos_data]
            
            # Estrategia 1: Buscar en lotes para evitar MemoryError
            tiempos_map = {}
            batch_size = 1000  # Procesar en lotes de 1000
            
            for i in range(0, len(tiempo_tuples), batch_size):
                batch_tuples = tiempo_tuples[i:i + batch_size]
                existing_tiempos = self._get_existing_tiempos_batch(session, batch_tuples)
                tiempos_map.update(existing_tiempos)
            
            # Estrategia 2: Insertar nuevos tiempos en lotes
            new_tiempos = [data for data in tiempos_data 
                         if (data['fecha'], data['hora'], data['minuto']) not in tiempos_map]
            
            for i in range(0, len(new_tiempos), batch_size):
                batch_new_tiempos = new_tiempos[i:i + batch_size]
                nuevos_ids = self._insert_new_tiempos_batch(session, batch_new_tiempos)
                tiempos_map.update(nuevos_ids)
            
            session.commit()
            return tiempos_map
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error en insert_or_get_tiempos: {e}")
            raise
        finally:
            session.close()
    
    def _get_existing_tiempos_batch(self, session, tiempo_tuples: List[tuple]) -> Dict[tuple, int]:
        """Busca tiempos existentes en lotes"""
        if not tiempo_tuples:
            return {}
        
        # Crear tabla temporal con los datos a buscar
        temp_table_query = """
            CREATE TEMP TABLE temp_tiempos_search (
                fecha DATE,
                hora INTEGER,
                minuto INTEGER
            ) ON COMMIT DROP
        """
        session.execute(text(temp_table_query))
        
        # Insertar datos en la tabla temporal en lotes
        insert_temp_query = "INSERT INTO temp_tiempos_search (fecha, hora, minuto) VALUES (:fecha, :hora, :minuto)"
        batch_params = [{'fecha': fecha, 'hora': hora, 'minuto': minuto} 
                       for fecha, hora, minuto in tiempo_tuples]
        
        for i in range(0, len(batch_params), 1000):
            session.execute(text(insert_temp_query), batch_params[i:i + 1000])
        
        # Buscar coincidencias
        search_query = """
            SELECT t.id_tiempo, t.fecha, t.hora, t.minuto 
            FROM dim_tiempo t
            INNER JOIN temp_tiempos_search ts ON t.fecha = ts.fecha AND t.hora = ts.hora AND t.minuto = ts.minuto
        """
        
        result = session.execute(text(search_query))
        return {(row['fecha'], row['hora'], row['minuto']): row['id_tiempo'] for row in result}
    
    def _insert_new_tiempos_batch(self, session, new_tiempos: List[Dict[str, Any]]) -> Dict[tuple, int]:
        """Inserta nuevos tiempos en lotes y retorna sus IDs"""
        if not new_tiempos:
            return {}
        
        nuevos_ids = {}
        
        for tiempo in new_tiempos:
            result = session.execute(
                text("""
                    INSERT INTO dim_tiempo (fecha, hora, minuto, cuarto_hora, clave_anio_mes)
                    VALUES (:fecha, :hora, :minuto, :cuarto_hora, :clave_anio_mes)
                    RETURNING id_tiempo
                """),
                {
                    'fecha': tiempo['fecha'],
                    'hora': tiempo['hora'],
                    'minuto': tiempo['minuto'],
                    'cuarto_hora': tiempo.get('cuarto_hora', (tiempo['hora'] * 4) + (tiempo['minuto'] // 15)),
                    'clave_anio_mes': tiempo.get('clave_anio_mes', tiempo['fecha'].strftime('%Y-%m'))
                }
            )
            
            tiempo_id = result.scalar()
            key = (tiempo['fecha'], tiempo['hora'], tiempo['minuto'])
            nuevos_ids[key] = tiempo_id
            self.logger.debug(f"Tiempo insertado: {key} (ID: {tiempo_id})")
        
        return nuevos_ids
    
class DataRepository:
    """Repositorio principal para inserción de datos"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.barra_repo = BarraRepository(db_connection)
        self.empresa_repo = EmpresaRepository(db_connection)
        self.tiempo_repo = TiempoRepository(db_connection)
        self.logger = logging.getLogger(__name__)
    
    def insert_precios_marginales(self, precios_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de precios marginales"""
        session = self.db.get_session()
        try:
            insert_query = """
                INSERT INTO precio_marginal (tiempo_id, barra_id, cmg_mills_kwh, cmg_usd_kwh, usd)
                VALUES (:tiempo_id, :barra_id, :cmg_mills_kwh, :cmg_usd_kwh, :usd)
            """
            
            for precio in precios_data:
                session.execute(text(insert_query), precio)
            
            session.commit()
            count = len(precios_data)
            self.logger.info(f"Insertados {count} registros en precio_marginal")
            return count
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error en insert_precios_marginales: {e}")
            raise
        finally:
            session.close()
    
    def insert_retiros_energia(self, retiros_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de retiros de energía"""
        session = self.db.get_session()
        try:
            insert_query = """
                INSERT INTO retiro_energia (tiempo_id, barra_id, suministrador_id, retiro_id, clave, tipo, medida_kwh)
                VALUES (:tiempo_id, :barra_id, :suministrador_id, :retiro_id, :clave, :tipo, :medida_kwh)
            """
            
            for retiro in retiros_data:
                session.execute(text(insert_query), retiro)
            
            session.commit()
            count = len(retiros_data)
            self.logger.info(f"Insertados {count} registros en retiro_energia")
            return count
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error en insert_retiros_energia: {e}")
            raise
        finally:
            session.close()
    
    def insert_contratos_fisicos(self, contratos_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de contratos físicos"""
        session = self.db.get_session()
        try:
            insert_query = """
                INSERT INTO contrato_fisico (tiempo_id, barra_id, clave, empresa_id, transaccion, kwh, valorizado_clp, id_contrato, cmg_peso_kwh)
                VALUES (:tiempo_id, :barra_id, :clave, :empresa_id, :transaccion, :kwh, :valorizado_clp, :id_contrato, :cmg_peso_kwh)
            """
            
            for contrato in contratos_data:
                session.execute(text(insert_query), contrato)
            
            session.commit()
            count = len(contratos_data)
            self.logger.info(f"Insertados {count} registros en contrato_fisico")
            return count
            
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error en insert_contratos_fisicos: {e}")
            raise
        finally:
            session.close()
