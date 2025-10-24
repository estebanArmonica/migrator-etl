import csv
import logging
from datetime import datetime
from typing import List, Dict, Any
import os
from tqdm import tqdm

class SimpleDataLoader:
    """Cargador de datos usando solo Python estándar (sin pandas)"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def debug_file_structure(self, file_path: str):
        """Función para debuggear la estructura del archivo"""
        self.logger.info("=== DEBUG FILE STRUCTURE ===")
        
        with open(file_path, 'r', encoding='latin-1') as file:
            # Leer primeras 5 líneas
            for i in range(5):
                line = file.readline()
                self.logger.info(f"Línea {i}: {line.strip()}")
        
        # Contar líneas totales
        with open(file_path, 'r', encoding='latin-1') as file:
            line_count = sum(1 for _ in file)
            self.logger.info(f"Total de líneas en archivo: {line_count}")
    
    
    def load_csv(self, file_path: str, encoding='utf-8') -> List[Dict[str, Any]]:
        """Carga un archivo CSV y retorna lista de diccionarios"""
        try:
            self.logger.info(f"Cargando archivo: {file_path}")
            
            # Verificar si el archivo existe y su tamaño
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Tamaño en MB
            self.logger.info(f"Tamaño del archivo: {file_size:.2f} MB")
            
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
                        
                        # Mostrar columnas disponibles para debugging
                        if reader.fieldnames:
                            self.logger.info(f"Columnas encontradas: {reader.fieldnames}")
                        
                        data = []
                        for row in reader:
                            data.append(row)
                            if len(data) % 100000 == 0:  # Log cada 100k registros
                                self.logger.info(f"Registros cargados: {len(data)}")
                        
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
        self.logger.info("Cargando datos de retiros de energía...")
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
        """Parsea fechas en diferentes formatos incluyendo YYMM"""
        if not date_str or str(date_str).lower() in ['nan', 'nat', 'none', '']:
            return None
        
        # Convertir a string y limpiar
        date_str = str(date_str).strip()
        
        # Si es un número, manejar formatos cortos
        if date_str.isdigit():
            length = len(date_str)
            
            if length == 4:  # YYMM
                try:
                    # 2410 -> 2024-10-01
                    return datetime.strptime(date_str + '01', '%y%m%d').date()
                except ValueError:
                    pass
                    
            elif length == 6:  # YYMMDD o YYYYMM
                try:
                    # 241001 -> 2024-10-01 (YYMMDD)
                    return datetime.strptime(date_str, '%y%m%d').date()
                except ValueError:
                    try:
                        # 202410 -> 2024-10-01 (YYYYMM)
                        return datetime.strptime(date_str + '01', '%Y%m%d').date()
                    except ValueError:
                        pass
                        
            elif length == 8:  # YYYYMMDD
                try:
                    return datetime.strptime(date_str, '%Y%m%d').date()
                except ValueError:
                    pass
        
        # Formatos de fecha estándar con separadores
        date_formats = [
            '%Y%m%d',        # 20241004
            '%Y-%m-%d',      # 2024-10-04
            '%d/%m/%Y',      # 04/10/2024
            '%m/%d/%Y',      # 10/04/2024
            '%d-%m-%Y',      # 04-10-2024
            '%Y/%m/%d',      # 2024/10/04
            '%d/%m/%y',      # 04/10/24
            '%m/%d/%y',      # 10/04/24
            '%d.%m.%Y',      # 04.10.2024
            '%d.%m.%y',      # 04.10.24
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        self.logger.warning(f"No se pudo parsear la fecha: {date_str}")
        return None

    def _validate_retiros_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Valida y limpia datos de retiros de energía con progreso"""
        if not data:
            self.logger.warning("No hay datos para validar")
            return []
            
        self.logger.info(f"Iniciando validación de {len(data)} registros de retiros...")
        cleaned_data = []
        
        # Verificar las claves requeridas en el primer registro
        if data:
            first_row = data[0]
            self.logger.info(f"Primera fila de ejemplo: {list(first_row.keys())}")
        
        # Definir posibles nombres de columnas (por problemas de encoding)
        possible_date_columns = ['Clave Año_Mes', 'Clave AÃ±o_Mes', 'Clave Anio_Mes', 'Fecha', 'fecha']
        
        required_keys_sets = [
            # Diferentes combinaciones posibles de columnas
            ['Cuarto de Hora', 'Barra', 'Suministrador', 'Retiro', 'clave', 'Tipo', 'Medida_kWh', 'Clave Año_Mes'],
            ['Cuarto de Hora', 'Barra', 'Suministrador', 'Retiro', 'clave', 'Tipo', 'Medida_kWh', 'Clave AÃ±o_Mes'],
            ['Cuarto de Hora', 'Barra', 'Suministrador', 'Retiro', 'clave', 'Tipo', 'Medida_kWh', 'Clave Anio_Mes'],
        ]
        
        # Encontrar qué conjunto de claves coincide
        valid_keys_set = None
        for key_set in required_keys_sets:
            if all(key in data[0] for key in key_set):
                valid_keys_set = key_set
                self.logger.info(f"Usando conjunto de claves: {valid_keys_set}")
                break
        
        if not valid_keys_set:
            self.logger.error("No se encontró un conjunto válido de columnas. Columnas disponibles:")
            self.logger.error(list(data[0].keys()))
            return []
        
        required_keys = valid_keys_set
        date_column = [key for key in possible_date_columns if key in data[0]][0] if any(key in data[0] for key in possible_date_columns) else None
        
        if not date_column:
            self.logger.error("No se encontró la columna de fecha")
            return []
            
        self.logger.info(f"Usando columna de fecha: '{date_column}'")
        
        # Usar tqdm para mostrar progreso
        for i, row in enumerate(tqdm(data, desc="Validando retiros")):
            try:
                # Validar campos requeridos (más eficiente)
                missing_keys = [key for key in required_keys if key not in row or not row[key]]
                if missing_keys:
                    if i < 10:  # Log solo las primeras 10 filas con errores
                        self.logger.warning(f"Fila {i}: Campos faltantes {missing_keys}")
                    continue
                
                # Convertir fecha
                fecha_str = str(row[date_column]).strip()
                fecha = self._parse_date(fecha_str)
                
                if not fecha:
                    if i < 10:  # Log solo las primeras 10 fechas problemáticas
                        self.logger.warning(f"Fila {i}: Fecha inválida '{fecha_str}'")
                    continue
                
                cleaned_row = {
                    'Cuarto de Hora': int(row['Cuarto de Hora']),
                    'Barra': row['Barra'].strip(),
                    'Suministrador': row['Suministrador'].strip(),
                    'Retiro': row['Retiro'].strip(),
                    'clave': row['clave'].strip(),
                    'Tipo': row['Tipo'].strip(),
                    'Medida_kWh': float(row['Medida_kWh']),
                    'Clave Año_Mes': fecha  # Estandarizar el nombre
                }
                cleaned_data.append(cleaned_row)
                
                # Log periódico del progreso
                if len(cleaned_data) % 100000 == 0:
                    self.logger.info(f"Retiros validados: {len(cleaned_data)}")
                
            except (ValueError, KeyError) as e:
                if i < 10:  # Log solo los primeros 10 errores
                    self.logger.warning(f"Fila {i} inválida en retiros: {e}")
                continue
        
        self.logger.info(f"Datos de retiros validados: {len(cleaned_data)} registros de {len(data)} originales")
        
        # Mostrar estadísticas de las primeras filas validadas
        if cleaned_data:
            self.logger.info("Primeras filas validadas (ejemplo):")
            for i in range(min(3, len(cleaned_data))):
                self.logger.info(f"Fila {i}: {cleaned_data[i]}")
        
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
