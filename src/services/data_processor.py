import pandas as pd
import logging
from typing import List, Dict, Any
from datetime import datetime
from tqdm import tqdm

class DataProcessor:
    """Procesador de datos para la migración"""
    
    def __init__(self, data_repository):
        self.repository = data_repository
        self.logger = logging.getLogger(__name__)
    
    def process_precios_marginales(self, df: pd.DataFrame) -> int:
        """Procesa y migra datos de precios marginales"""
        self.logger.info("Procesando datos de precios marginales...")
        
        # Extraer datos únicos
        barras_unicas = df['BARRA'].unique().tolist()
        tiempos_data = []
        
        for _, row in df.iterrows():
            tiempos_data.append({
                'fecha': row['FECHA'],
                'hora': int(row['HORA']),
                'minuto': int(row['MINUTO']),
                'cuarto_hora': (int(row['HORA']) * 4) + (int(row['MINUTO']) // 15)
            })
        
        # Obtener mapeos
        barras_map = self.repository.barra_repo.insert_or_get_barras(barras_unicas)
        tiempos_map = self.repository.tiempo_repo.insert_or_get_tiempos(tiempos_data)
        
        # Preparar datos para inserción
        precios_to_insert = []
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Procesando precios"):
            tiempo_key = (row['FECHA'], int(row['HORA']), int(row['MINUTO']))
            tiempo_id = tiempos_map.get(tiempo_key)
            barra_id = barras_map.get(row['BARRA'])
            
            if tiempo_id and barra_id:
                precios_to_insert.append({
                    'tiempo_id': tiempo_id,
                    'barra_id': barra_id,
                    'cmg_mills_kwh': float(row['CMg[mills/kWh]']),
                    'cmg_usd_kwh': float(row['CMg[$/KWh]']),
                    'usd': float(row['USD'])
                })
        
        # Insertar datos
        return self.repository.insert_precios_marginales(precios_to_insert)
    
    def process_retiros_energia(self, df: pd.DataFrame) -> int:
        """Procesa y migra datos de retiros de energía"""
        self.logger.info("Procesando datos de retiros de energía...")
        
        # Extraer datos únicos
        barras_unicas = df['Barra'].unique().tolist()
        empresas_unicas = df['Suministrador'].unique().tolist() + df['Retiro'].unique().tolist()
        
        tiempos_data = []
        for _, row in df.iterrows():
            tiempos_data.append({
                'fecha': row['Clave Año_Mes'],
                'hora': (int(row['Cuarto de Hora']) - 1) // 4,
                'minuto': ((int(row['Cuarto de Hora']) - 1) % 4) * 15,
                'cuarto_hora': int(row['Cuarto de Hora']),
                'clave_anio_mes': row['Clave Año_Mes'].strftime('%Y-%m')
            })
        
        # Obtener mapeos
        barras_map = self.repository.barra_repo.insert_or_get_barras(barras_unicas)
        empresas_map = self.repository.empresa_repo.insert_or_get_empresas(empresas_unicas)
        tiempos_map = self.repository.tiempo_repo.insert_or_get_tiempos(tiempos_data)
        
        # Preparar datos para inserción
        retiros_to_insert = []
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Procesando retiros"):
            hora = (int(row['Cuarto de Hora']) - 1) // 4
            minuto = ((int(row['Cuarto de Hora']) - 1) % 4) * 15
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
                    'medida_kwh': float(row['Medida_kWh'])
                })
        
        # Insertar datos
        return self.repository.insert_retiros_energia(retiros_to_insert)
    
    def process_contratos_fisicos(self, df: pd.DataFrame) -> int:
        """Procesa y migra datos de contratos físicos"""
        self.logger.info("Procesando datos de contratos físicos...")
        
        # Extraer datos únicos
        barras_unicas = df['Barra'].unique().tolist()
        empresas_unicas = df['Empresa'].unique().tolist()
        
        tiempos_data = []
        for _, row in df.iterrows():
            tiempos_data.append({
                'fecha': datetime.now().date(),  # Fecha por defecto, ajustar según datos reales
                'hora': (int(row['Cuarto de Hora']) - 1) // 4,
                'minuto': ((int(row['Cuarto de Hora']) - 1) % 4) * 15,
                'cuarto_hora': int(row['Cuarto de Hora'])
            })
        
        # Obtener mapeos
        barras_map = self.repository.barra_repo.insert_or_get_barras(barras_unicas)
        empresas_map = self.repository.empresa_repo.insert_or_get_empresas(empresas_unicas)
        tiempos_map = self.repository.tiempo_repo.insert_or_get_tiempos(tiempos_data)
        
        # Preparar datos para inserción
        contratos_to_insert = []
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Procesando contratos"):
            hora = (int(row['Cuarto de Hora']) - 1) // 4
            minuto = ((int(row['Cuarto de Hora']) - 1) % 4) * 15
            tiempo_key = (datetime.now().date(), hora, minuto)  # Ajustar fecha según datos
            
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
                    'kwh': float(row['Kwhh']),
                    'valorizado_clp': float(row['Valorizado_CLP']),
                    'id_contrato': int(row['Id_Contrato']),
                    'cmg_peso_kwh': float(row['CMG_PESO_KWH'])
                })
        
        # Insertar datos
        return self.repository.insert_contratos_fisicos(contratos_to_insert)