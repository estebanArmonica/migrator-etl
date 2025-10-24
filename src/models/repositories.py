import logging
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pandas as pd

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
            
            # Accedemos por posición en lugar de por nombre de columna
            existing_barras = {row[1]: row[0] for row in result}
            
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
            tiempos_map = self._get_existing_tiempos_batch(session, tiempo_tuples)
            
            # Identifica nuevos tiempos a insertar
            new_tiempos = [data for data in tiempos_data
                           if (data['fecha'], data['hora'], data['minuto']) not in tiempos_map]
            
            # Insertar nuevos tiempos
            if new_tiempos:
                nuevos_ids = self._insert_new_tiempos_batch(session, new_tiempos)
                tiempos_map.update(nuevos_ids)
            
            session.commit()
            self.logger.info(f"Procesados {len(tiempos_data)} tiempos: {len(tiempos_map)} existentes,  nuevos: {len(new_tiempos)}")
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
        try:
            # Crear tabla temporal con los datos a buscar
            temp_table_query = """
                CREATE TEMP TABLE IF NOT EXISTS temp_tiempos_search (
                    fecha DATE,
                    hora INTEGER,
                    minuto INTEGER
                ) ON COMMIT DROP
            """
            session.execute(text(temp_table_query))
            
            # Insertar datos en la tabla temporal
            insert_temp_query = "INSERT INTO temp_tiempos_search (fecha, hora, minuto) VALUES (:fecha, :hora, :minuto)"
                
            # Insertar en lotes para mejor performance
            batch_size = 1000
            for i in range(0, len(tiempo_tuples), batch_size):
                batch = tiempo_tuples[i:i + batch_size]
                params = [{'fecha': fecha, 'hora': hora, 'minuto': minuto} 
                        for fecha, hora, minuto in batch]
                session.execute(text(insert_temp_query), params)
            
            # Buscar coincidencias
            search_query = """
                SELECT t.id_tiempo, t.fecha, t.hora, t.minuto 
                FROM dim_tiempo t
                INNER JOIN temp_tiempos_search ts ON 
                    t.fecha = ts.fecha AND 
                    t.hora = ts.hora AND 
                    t.minuto = ts.minuto
            """
            
            result = session.execute(text(search_query))
            tiempos_map = {}
            for row in result.mappings():
                key = (row['fecha'], row['hora'], row['minuto'])
                tiempos_map[key] = row['id_tiempo']
                
            self.logger.debug(f"Encontrados {len(tiempos_map)} tiempos existentes")
            return tiempos_map
        except Exception as e:
            self.logger.error(f"Error en _get_existing_tiempos_batch: {e}")
            session.rollback()
            return {}
    
    def _insert_new_tiempos_batch(self, session, new_tiempos: List[Dict[str, Any]]) -> Dict[tuple, int]:
        """Inserta nuevos tiempos en lotes y retorna sus IDs"""
        if not new_tiempos:
            return {}
        
        nuevos_ids = {}
        
        try:
            # Insertar uno por uno para poder obtener los RETURNING values
            insert_query = text("""
                INSERT INTO dim_tiempo (fecha, hora, minuto, cuarto_hora, clave_anio_mes)
                VALUES (:fecha, :hora, :minuto, :cuarto_hora, :clave_anio_mes)
                ON CONFLICT (fecha, hora, minuto) DO NOTHING
                RETURNING id_tiempo, fecha, hora, minuto
            """)
            
            # Insertar cada tiempo individualmente para capturar los RETURNING
            for tiempo in new_tiempos:
                params = {
                    'fecha': tiempo['fecha'],
                    'hora': tiempo['hora'],
                    'minuto': tiempo['minuto'],
                    'cuarto_hora': tiempo.get('cuarto_hora', (tiempo['hora'] * 4) + (tiempo['minuto'] // 15)),
                    'clave_anio_mes': tiempo.get('clave_anio_mes', tiempo['fecha'].strftime('%Y-%m'))
                }
                
                result = session.execute(insert_query, params)
                row = result.fetchone()
                
                if row:
                    # Se insertó correctamente, capturar el ID
                    key = (row[1], row[2], row[3])  # fecha, hora, minuto
                    nuevos_ids[key] = row[0]  # id_tiempo
                    self.logger.debug(f"Tiempo insertado: {key} (ID: {row[0]})")
            
            # Si hay registros que no se insertaron (por conflicto), obtener sus IDs
            inserted_keys = set(nuevos_ids.keys())
            all_keys = {(t['fecha'], t['hora'], t['minuto']) for t in new_tiempos}
            missing_keys = all_keys - inserted_keys
            
            if missing_keys:
                self.logger.info(f"Recuperando {len(missing_keys)} IDs de tiempos que ya existían")
                # Obtener los IDs de los registros que ya existían
                missing_tiempos_map = self._get_existing_tiempos_batch(session, list(missing_keys))
                nuevos_ids.update(missing_tiempos_map)
            
            self.logger.info(f"Insertados/recuperados {len(nuevos_ids)} nuevos tiempos")
            return nuevos_ids
            
        except Exception as e:
            self.logger.error(f"Error en _insert_new_tiempos_batch: {e}")
            raise
    
class DataRepository:
    """Repositorio principal para inserción de datos"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.barra_repo = BarraRepository(db_connection)
        self.tiempo_repo = TiempoRepository(db_connection)
        self.logger = logging.getLogger(__name__)
        
    def _parse_fecha_problematica(self, fecha_str):
        """Parse fechas en formatos problemáticos como 2410"""
        try:
            # Si es un número como 2410, asumimos YYMM y agregar día 01
            if isinstance(fecha_str, (int, float)) and 100 <= fecha_str <= 9999:
                fecha_str = str(int(fecha_str))
                if len(fecha_str) == 4:
                    # Formato YYMM -> asumimos YYMMDD con DD=01
                    return datetime.strptime(fecha_str + '01', '%y%m%d').date()
                elif len(fecha_str) == 3:
                    # Formato YMD donde Y es 2? - manejar caso especial
                    return datetime.strptime('0' + fecha_str, '%y%m%d').date()
            
            # Si es string, intentar diferentes formatos
            if isinstance(fecha_str, str):
                fecha_str = str(fecha_str).strip()
                
                # Formato YYMM
                if len(fecha_str) == 4 and fecha_str.isdigit():
                    return datetime.strptime(fecha_str + '01', '%y%m%d').date()
                
                # Formato YYMMDD
                elif len(fecha_str) == 6 and fecha_str.isdigit():
                    return datetime.strptime(fecha_str, '%y%m%d').date()
                
                # Formato estándar
                else:
                    return pd.to_datetime(fecha_str).date()
                    
            # Si ya es datetime o date
            elif isinstance(fecha_str, (datetime, pd.Timestamp)):
                return fecha_str.date()
            elif isinstance(fecha_str, pd._libs.tslibs.nattype.NaTType):
                return None
                
        except Exception as e:
            self.logger.warning(f"No se pudo parsear la fecha: {fecha_str}. Error: {e}")
            return None
        
        return None
    
    def process_retiros_energia(self, data_retiros):
        """Procesa datos de retiros de energía"""
        processed_data = []
        
        for item in data_retiros:
            try:
                # Parsear fecha problemática
                fecha = self._parse_fecha_problematica(item.get('fecha'))
                if fecha is None:
                    self.logger.warning(f"Fecha inválida omitida: {item.get('fecha')}")
                    continue
                
                # Validar otros campos requeridos
                hora = item.get('hora', 0)
                minuto = item.get('minuto', 0)
                barra_nombre = item.get('barra')
                
                if not barra_nombre:
                    self.logger.warning("Registro omitido: falta nombre de barra")
                    continue
                
                processed_data.append({
                    'fecha': fecha,
                    'hora': int(hora) if hora is not None else 0,
                    'minuto': int(minuto) if minuto is not None else 0,
                    'barra': barra_nombre,
                    'suministrador': item.get('suministrador'),
                    'retiro': item.get('retiro'), 
                    'clave': item.get('clave'),
                    'tipo': item.get('tipo'),
                    'medida_kwh': float(item.get('medida_kwh', 0)) if item.get('medida_kwh') not in [None, ''] else 0.0
                })
                
            except Exception as e:
                self.logger.error(f"Error procesando registro de retiros: {item}. Error: {e}")
                continue
        
        return processed_data
    
    def insert_precios_marginales(self, precios_data: List[Dict[str, Any]]) -> int:
        """Inserta datos de precios marginales"""
        session = self.db.get_session()
        try:
            insert_query = """
                INSERT INTO costo_marginal (tiempo_id, barra_id, cmg_mills_kwh, cmg_usd_kwh, usd)
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
                INSERT INTO retiro_energia (tiempo_id, barra_id, suministrador, retiro, clave, tipo, clave_anio_mes, medida_kwh)
                VALUES (:tiempo_id, :barra_id, :suministrador, :retiro, :clave, :tipo, :clave_anio_mes, :medida_kwh)
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
                INSERT INTO contrato_fisico (tiempo_id, barra_id, clave, nom_empresa, transaccion, kwh, valorizado_clp, id_contrato, cmg_peso_kwh)
                VALUES (:tiempo_id, :barra_id, :clave, :nom_empresa, :transaccion, :kwh, :valorizado_clp, :id_contrato, :cmg_peso_kwh)
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
