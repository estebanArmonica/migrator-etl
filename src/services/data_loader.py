import csv
import logging
from datetime import datetime
from typing import List, Dict, Any
import os

class SimpleDataLoader:
    """Cargador de datos usando solo Python estándar (sin pandas)"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def load_csv(self, file_path: str, encoding='utf-8') -> List[Dict[str, Any]]:
        """Carga un archivo CSV y retorna lista de diccionarios"""
        try:
            self.logger.info(f"Cargando archivo: {file_path}")
            
            # Intentar diferentes encodings
            encodings = [encoding, 'latin-1', 'utf-8-sig', 'iso-8859-1']
            
            for enc in encodings:
                try:
                    with open(file_path, 'r', encoding=enc) as file:
                        # Detectar delimitador
                        sample = file.read(4096)
                        file.seek(0)
                        delimiter = ',' if sample.count(',') > sample.count(';') else ';'
                        
                        # Leer CSV
                        reader = csv.DictReader(file, delimiter=delimiter)
                        data = [row for row in reader]
                        
                        self.logger.info(f"Archivo cargado con encoding {enc}: {len(data)} registros")
                        return data
                        
                except UnicodeDecodeError:
                    continue
            
            raise ValueError(f"No se pudo decodificar el archivo {file_path} con los encodings probados")
            
        except Exception as e:
            self.logger.error(f"Error cargando archivo {file_path}: {e}")
            return []
    
    def load_precios_marginales(self, file_path: str) -> List[Dict[str, Any]]:
        """Carga datos de precios marginales"""
        data = self.load_csv(file_path)
        return self._validate_precios_data(data)
    
    def load_retiros_energia(self, file_path: str) -> List[Dict[str, Any]]:
        """Carga datos de retiros de energía"""
        data = self.load_csv(file_path)
        return self._validate_retiros_data(data)
    
    def load_contratos_fisicos(self, file_path: str) -> List[Dict[str, Any]]:
        """Carga datos de contratos físicos"""
        data = self.load_csv(file_path)
        return self._validate_contratos_data(data)
    
    def _validate_precios_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Valida y limpia datos de precios marginales"""
        cleaned_data = []
        
        for row in data:
            try:
                # Validar campos requeridos
                if not all(key in row for key in ['FECHA', 'HORA', 'MINUTO', 'BARRA']):
                    continue
                
                # Convertir fecha - manejar múltiples formatos
                fecha_str = str(row['FECHA']).strip()
                fecha = self._parse_date(fecha_str)
                
                if not fecha:
                    continue
                
                # Convertir tipos
                cleaned_row = {
                    'FECHA': fecha,
                    'HORA': int(row['HORA']),
                    'MINUTO': int(row['MINUTO']),
                    'BARRA': row['BARRA'].strip(),
                    'CMg[mills/kWh]': float(row['CMg[mills/kWh]']),
                    'CMg[$/KWh]': float(row['CMg[$/KWh]']),
                    'USD': float(row['USD'])
                }
                cleaned_data.append(cleaned_row)
                
            except (ValueError, KeyError) as e:
                self.logger.warning(f"Fila inválida en precios marginales: {e}")
                continue
        
        self.logger.info(f"Datos de precios marginales validados: {len(cleaned_data)} registros")
        return cleaned_data
    
    def _parse_date(self, date_str: str) -> datetime.date:
        """Parsea fechas en diferentes formatos"""
        date_formats = [
            '%Y%m%d',        # 20241004
            '%Y-%m-%d',      # 2024-10-04
            '%d/%m/%Y',      # 04/10/2024
            '%m/%d/%Y',      # 10/04/2024
            '%d-%m-%Y',      # 04-10-2024
            '%Y/%m/%d',      # 2024/10/04
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        self.logger.warning(f"No se pudo parsear la fecha: {date_str}")
        return None

    def _validate_retiros_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Valida y limpia datos de retiros de energía"""
        cleaned_data = []
        
        for row in data:
            try:
                # Validar campos requeridos
                required_keys = ['Cuarto de Hora', 'Barra', 'Suministrador', 'Retiro', 'clave', 'Tipo', 'Medida_kWh', 'Clave Año_Mes']
                if not all(key in row for key in required_keys):
                    continue
                
                # Convertir fecha
                fecha_str = str(row['Clave Año_Mes']).strip()
                fecha = self._parse_date(fecha_str)
                
                if not fecha:
                    continue
                
                cleaned_row = {
                    'Cuarto de Hora': int(row['Cuarto de Hora']),
                    'Barra': row['Barra'].strip(),
                    'Suministrador': row['Suministrador'].strip(),
                    'Retiro': row['Retiro'].strip(),
                    'clave': row['clave'].strip(),
                    'Tipo': row['Tipo'].strip(),
                    'Medida_kWh': float(row['Medida_kWh']),
                    'Clave Año_Mes': fecha
                }
                cleaned_data.append(cleaned_row)
                
            except (ValueError, KeyError) as e:
                self.logger.warning(f"Fila inválida en retiros: {e}")
                continue
        
        self.logger.info(f"Datos de retiros validados: {len(cleaned_data)} registros")
        return cleaned_data

    def _validate_contratos_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Valida y limpia datos de contratos físicos"""
        cleaned_data = []
        
        for row in data:
            try:
                # Validar campos requeridos
                required_keys = ['Cuarto de Hora', 'Barra', 'clave', 'Empresa', 'TransacciÃ³n', 'Kwhh', 'Valorizado_CLP', 'Id_Contrato', 'CMG_PESO_KWH']
                if not all(key in row for key in required_keys):
                    continue
                
                cleaned_row = {
                    'Cuarto de Hora': int(row['Cuarto de Hora']),
                    'Barra': row['Barra'].strip(),
                    'clave': row['clave'].strip(),
                    'Empresa': row['Empresa'].strip(),
                    'TransacciÃ³n': row['TransacciÃ³n'].strip(),
                    'Kwhh': float(row['Kwhh']),
                    'Valorizado_CLP': float(row['Valorizado_CLP']),
                    'Id_Contrato': int(row['Id_Contrato']),
                    'CMG_PESO_KWH': float(row['CMG_PESO_KWH'])
                }
                cleaned_data.append(cleaned_row)
                
            except (ValueError, KeyError) as e:
                self.logger.warning(f"Fila inválida en contratos: {e}")
                continue
        
        self.logger.info(f"Datos de contratos validados: {len(cleaned_data)} registros")
        return cleaned_data
