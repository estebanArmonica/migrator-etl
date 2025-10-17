import pandas as pd
from datetime import datetime
import logging

class DataValidator:
    """Validador de datos para la migración"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _parse_date_flexible(self, date_str):
        """Parsea fechas en diferentes formatos para pandas"""
        from datetime import datetime
        
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
                return datetime.strptime(str(date_str), fmt).date()
            except ValueError:
                continue
        
        return None
    
    def validate_precio_marginal_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida y limpia datos de precio marginal con formato de fecha flexible"""
        required_columns = ['FECHA', 'HORA', 'MINUTO', 'BARRA', 'CMg[mills/kWh]', 'CMg[$/KWh]', 'USD']
        
        # Verificar columnas requeridas
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columnas faltantes en precio marginal: {missing_columns}")
        
        # Limpiar datos
        df_clean = df.copy()
        
        # Convertir fecha con formato flexible
        df_clean['FECHA'] = df_clean['FECHA'].apply(self._parse_date_flexible)
        
        # Convertir otros tipos de datos
        df_clean['HORA'] = pd.to_numeric(df_clean['HORA'], errors='coerce')
        df_clean['MINUTO'] = pd.to_numeric(df_clean['MINUTO'], errors='coerce')
        df_clean['CMg[mills/kWh]'] = pd.to_numeric(df_clean['CMg[mills/kWh]'], errors='coerce')
        df_clean['CMg[$/KWh]'] = pd.to_numeric(df_clean['CMg[$/KWh]'], errors='coerce')
        df_clean['USD'] = pd.to_numeric(df_clean['USD'], errors='coerce')
        
        # Eliminar filas con valores nulos en campos críticos
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=['FECHA', 'HORA', 'MINUTO', 'BARRA'])
        final_count = len(df_clean)
        
        if initial_count != final_count:
            self.logger.warning(f"Se eliminaron {initial_count - final_count} filas con datos inválidos en precio marginal")
        
        return df_clean


    def validate_retiros_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida y limpia datos de retiros de energía"""
        required_columns = ['Cuarto de Hora', 'Barra', 'Suministrador', 'Retiro', 'clave', 'Tipo', 'Medida_kWh', 'Clave Año_Mes']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columnas faltantes en retiros: {missing_columns}")
        
        df_clean = df.copy()
        
        # Convertir tipos de datos
        df_clean['Cuarto de Hora'] = pd.to_numeric(df_clean['Cuarto de Hora'], errors='coerce')
        df_clean['Medida_kWh'] = pd.to_numeric(df_clean['Medida_kWh'], errors='coerce')
        df_clean['Clave Año_Mes'] = pd.to_datetime(df_clean['Clave Año_Mes'], errors='coerce')
        
        # Validar valores
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=['Barra', 'Suministrador', 'Retiro', 'clave'])
        final_count = len(df_clean)
        
        if initial_count != final_count:
            self.logger.warning(f"Se eliminaron {initial_count - final_count} filas con datos inválidos en retiros")
        
        return df_clean
    
    def validate_contratos_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida y limpia datos de contratos físicos"""
        required_columns = ['Cuarto de Hora', 'Barra', 'clave', 'Empresa', 'TransacciÃ³n', 'Kwhh', 
                          'Valorizado_CLP', 'Id_Contrato', 'CMG_PESO_KWH']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columnas faltantes en contratos: {missing_columns}")
        
        df_clean = df.copy()
        
        # Convertir tipos de datos
        df_clean['Cuarto de Hora'] = pd.to_numeric(df_clean['Cuarto de Hora'], errors='coerce')
        df_clean['Kwhh'] = pd.to_numeric(df_clean['Kwhh'], errors='coerce')
        df_clean['Valorizado_CLP'] = pd.to_numeric(df_clean['Valorizado_CLP'], errors='coerce')
        df_clean['Id_Contrato'] = pd.to_numeric(df_clean['Id_Contrato'], errors='coerce')
        df_clean['CMG_PESO_KWH'] = pd.to_numeric(df_clean['CMG_PESO_KWH'], errors='coerce')
        
        # Validar valores
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=['Barra', 'clave', 'Empresa', 'TransacciÃ³n'])
        final_count = len(df_clean)
        
        if initial_count != final_count:
            self.logger.warning(f"Se eliminaron {initial_count - final_count} filas con datos inválidos en contratos")
        
        return df_clean
