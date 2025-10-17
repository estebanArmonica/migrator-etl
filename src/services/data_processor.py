import logging
from typing import List, Dict, Any
from datetime import datetime
from tqdm import tqdm

class SimpleDataProcessor:
    """Procesador de datos optimizado para grandes volúmenes"""
    
    def __init__(self, data_repository):
        self.repository = data_repository
        self.logger = logging.getLogger(__name__)
        self.batch_size = 5000  # Procesar en lotes de 5000 registros
    
    def process_precios_marginales(self, data: List[Dict[str, Any]]) -> int:
        """Procesa y migra datos de precios marginales en lotes"""
        self.logger.info("Procesando datos de precios marginales...")
        
        if not data:
            self.logger.warning("No hay datos para procesar")
            return 0
        
        total_processed = 0
        
        # Procesar en lotes
        for batch_num, i in enumerate(range(0, len(data), self.batch_size), 1):
            batch_data = data[i:i + self.batch_size]
            self.logger.info(f"Procesando lote {batch_num} de precios marginales ({len(batch_data)} registros)")
            
            processed_in_batch = self._process_precios_batch(batch_data)
            total_processed += processed_in_batch
            
            self.logger.info(f"Lote {batch_num} completado: {processed_in_batch} registros")
        
        self.logger.info(f"Total precios marginales procesados: {total_processed}")
        return total_processed
    
    def _process_precios_batch(self, batch_data: List[Dict[str, Any]]) -> int:
        """Procesa un lote de datos de precios marginales"""
        # Extraer datos únicos del lote
        barras_unicas = list(set(row['BARRA'] for row in batch_data))
        tiempos_data = []
        
        for row in batch_data:
            tiempos_data.append({
                'fecha': row['FECHA'],
                'hora': row['HORA'],
                'minuto': row['MINUTO'],
                'cuarto_hora': (row['HORA'] * 4) + (row['MINUTO'] // 15)
            })
        
        # Obtener mapeos
        barras_map = self.repository.barra_repo.insert_or_get_barras(barras_unicas)
        tiempos_map = self.repository.tiempo_repo.insert_or_get_tiempos(tiempos_data)
        
        # Preparar datos para inserción
        precios_to_insert = []
        
        for row in batch_data:
            tiempo_key = (row['FECHA'], row['HORA'], row['MINUTO'])
            tiempo_id = tiempos_map.get(tiempo_key)
            barra_id = barras_map.get(row['BARRA'])
            
            if tiempo_id and barra_id:
                precios_to_insert.append({
                    'tiempo_id': tiempo_id,
                    'barra_id': barra_id,
                    'cmg_mills_kwh': row['CMg[mills/kWh]'],
                    'cmg_usd_kwh': row['CMg[$/KWh]'],
                    'usd': row['USD']
                })
        
        # Insertar datos del lote
        if precios_to_insert:
            return self.repository.insert_precios_marginales(precios_to_insert)
        return 0
    
    def process_retiros_energia(self, data: List[Dict[str, Any]]) -> int:
        """Procesa y migra datos de retiros de energía en lotes"""
        self.logger.info("Procesando datos de retiros de energía...")
        
        if not data:
            self.logger.warning("No hay datos para procesar")
            return 0
        
        total_processed = 0
        
        # Procesar en lotes
        for batch_num, i in enumerate(range(0, len(data), self.batch_size), 1):
            batch_data = data[i:i + self.batch_size]
            self.logger.info(f"Procesando lote {batch_num} de retiros ({len(batch_data)} registros)")
            
            processed_in_batch = self._process_retiros_batch(batch_data)
            total_processed += processed_in_batch
            
            self.logger.info(f"Lote {batch_num} completado: {processed_in_batch} registros")
        
        self.logger.info(f"Total retiros procesados: {total_processed}")
        return total_processed
    
    def _process_retiros_batch(self, batch_data: List[Dict[str, Any]]) -> int:
        """Procesa un lote de datos de retiros de energía"""
        # Extraer datos únicos del lote
        barras_unicas = list(set(row['Barra'] for row in batch_data))
        empresas_unicas = list(set(row['Suministrador'] for row in batch_data) | 
                              set(row['Retiro'] for row in batch_data))
        
        tiempos_data = []
        for row in batch_data:
            tiempos_data.append({
                'fecha': row['Clave Año_Mes'],
                'hora': (row['Cuarto de Hora'] - 1) // 4,
                'minuto': ((row['Cuarto de Hora'] - 1) % 4) * 15,
                'cuarto_hora': row['Cuarto de Hora'],
                'clave_anio_mes': row['Clave Año_Mes'].strftime('%Y-%m')
            })
        
        # Obtener mapeos
        barras_map = self.repository.barra_repo.insert_or_get_barras(barras_unicas)
        empresas_map = self.repository.empresa_repo.insert_or_get_empresas(empresas_unicas)
        tiempos_map = self.repository.tiempo_repo.insert_or_get_tiempos(tiempos_data)
        
        # Preparar datos para inserción
        retiros_to_insert = []
        
        for row in batch_data:
            hora = (row['Cuarto de Hora'] - 1) // 4
            minuto = ((row['Cuarto de Hora'] - 1) % 4) * 15
            tiempo_key = (row['Clave Año_Mes'], hora, minuto)
            
            tiempo_id = tiempos_map.get(tiempo_key)
            barra_id = barras_map.get(row['Barra'])
            suministrador_id = empresas_map.get(row['Suministrador'])
            retiro_id = empresas_map.get(row['Retiro'])
            
            if all([tiempo_id, barra_id, suministrador_id, retiro_id]):
                retiros_to_insert.append({
                    'tiempo_id': tiempo_id,
                    'barra_id': barra_id,
                    'suministrador_id': suministrador_id,
                    'retiro_id': retiro_id,
                    'clave': row['clave'],
                    'tipo': row['Tipo'],
                    'medida_kwh': row['Medida_kWh']
                })
        
        # Insertar datos del lote
        if retiros_to_insert:
            return self.repository.insert_retiros_energia(retiros_to_insert)
        return 0
    
    def process_contratos_fisicos(self, data: List[Dict[str, Any]]) -> int:
        """Procesa y migra datos de contratos físicos en lotes"""
        self.logger.info("Procesando datos de contratos físicos...")
        
        if not data:
            self.logger.warning("No hay datos para procesar")
            return 0
        
        total_processed = 0
        
        # Procesar en lotes
        for batch_num, i in enumerate(range(0, len(data), self.batch_size), 1):
            batch_data = data[i:i + self.batch_size]
            self.logger.info(f"Procesando lote {batch_num} de contratos ({len(batch_data)} registros)")
            
            processed_in_batch = self._process_contratos_batch(batch_data)
            total_processed += processed_in_batch
            
            self.logger.info(f"Lote {batch_num} completado: {processed_in_batch} registros")
        
        self.logger.info(f"Total contratos procesados: {total_processed}")
        return total_processed
    
    def _process_contratos_batch(self, batch_data: List[Dict[str, Any]]) -> int:
        """Procesa un lote de datos de contratos físicos"""
        # Extraer datos únicos del lote
        barras_unicas = list(set(row['Barra'] for row in batch_data))
        empresas_unicas = list(set(row['Empresa'] for row in batch_data))
        
        tiempos_data = []
        for row in batch_data:
            tiempos_data.append({
                'fecha': datetime.now().date(),
                'hora': (row['Cuarto de Hora'] - 1) // 4,
                'minuto': ((row['Cuarto de Hora'] - 1) % 4) * 15,
                'cuarto_hora': row['Cuarto de Hora']
            })
        
        # Obtener mapeos
        barras_map = self.repository.barra_repo.insert_or_get_barras(barras_unicas)
        empresas_map = self.repository.empresa_repo.insert_or_get_empresas(empresas_unicas)
        tiempos_map = self.repository.tiempo_repo.insert_or_get_tiempos(tiempos_data)
        
        # Preparar datos para inserción
        contratos_to_insert = []
        
        for row in batch_data:
            hora = (row['Cuarto de Hora'] - 1) // 4
            minuto = ((row['Cuarto de Hora'] - 1) % 4) * 15
            tiempo_key = (datetime.now().date(), hora, minuto)
            
            tiempo_id = tiempos_map.get(tiempo_key)
            barra_id = barras_map.get(row['Barra'])
            empresa_id = empresas_map.get(row['Empresa'])
            
            if all([tiempo_id, barra_id, empresa_id]):
                contratos_to_insert.append({
                    'tiempo_id': tiempo_id,
                    'barra_id': barra_id,
                    'clave': row['clave'],
                    'empresa_id': empresa_id,
                    'transaccion': row['TransacciÃ³n'],
                    'kwh': row['Kwhh'],
                    'valorizado_clp': row['Valorizado_CLP'],
                    'id_contrato': row['Id_Contrato'],
                    'cmg_peso_kwh': row['CMG_PESO_KWH']
                })
        
        # Insertar datos del lote
        if contratos_to_insert:
            return self.repository.insert_contratos_fisicos(contratos_to_insert)
        return 0
