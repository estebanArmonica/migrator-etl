import pandas as pd
import logging
import os
from typing import Optional
from src.utils.validators import DataValidator

class DataLoader:
    """Cargador de datos desde archivos CSV"""
    
    def __init__(self):
        self.validator = DataValidator()
        self.logger = logging.getLogger(__name__)
    
    def load_precios_marginales(self, file_path: str) -> Optional[pd.DataFrame]:
        """Carga datos de precios marginales desde CSV"""
        try:
            self.logger.info(f"Cargando archivo: {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Verificar encoding alternativo si falla
            if df.empty:
                df = pd.read_csv(file_path, encoding='latin-1')
            
            df_clean = self.validator.validate_precio_marginal_data(df)
            self.logger.info(f"Datos de precios marginales cargados: {len(df_clean)} registros")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cargando precios marginales: {e}")
            return None
    
    def load_retiros_energia(self, file_path: str) -> Optional[pd.DataFrame]:
        """Carga datos de retiros de energía desde CSV"""
        try:
            self.logger.info(f"Cargando archivo: {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8')
            
            if df.empty:
                df = pd.read_csv(file_path, encoding='latin-1')
            
            df_clean = self.validator.validate_retiros_data(df)
            self.logger.info(f"Datos de retiros cargados: {len(df_clean)} registros")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cargando retiros de energía: {e}")
            return None
    
    def load_contratos_fisicos(self, file_path: str) -> Optional[pd.DataFrame]:
        """Carga datos de contratos físicos desde CSV"""
        try:
            self.logger.info(f"Cargando archivo: {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8')
            
            if df.empty:
                df = pd.read_csv(file_path, encoding='latin-1')
            
            df_clean = self.validator.validate_contratos_data(df)
            self.logger.info(f"Datos de contratos cargados: {len(df_clean)} registros")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error cargando contratos físicos: {e}")
            return None