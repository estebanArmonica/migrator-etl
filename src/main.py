import logging
import sys
import os
from datetime import datetime

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.config.database import DatabaseConnection
from src.models.repositories import DataRepository
from src.services.data_loader import DataLoader
from src.services.data_processor import DataProcessor
from src.utils.logger import setup_logger

class MigracionApp:
    """Aplicación principal de migración de datos"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.db_connection = DatabaseConnection()
        self.data_repository = DataRepository(self.db_connection)
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor(self.data_repository)
    
    def run_migracion(self, archivos_config: dict):
        """Ejecuta el proceso completo de migración"""
        self.logger.info("Iniciando proceso de migración...")
        
        total_registros = 0
        
        try:
            # Migrar Precios Marginales
            if archivos_config.get('precios_marginales'):
                self.logger.info("=== MIGRANDO PRECIOS MARGINALES ===")
                df_precios = self.data_loader.load_precios_marginales(archivos_config['precios_marginales'])
                if df_precios is not None:
                    count = self.data_processor.process_precios_marginales(df_precios)
                    total_registros += count
                    self.logger.info(f"Precios marginales migrados: {count} registros")
            
            # Migrar Retiros de Energía
            if archivos_config.get('retiros_energia'):
                self.logger.info("=== MIGRANDO RETIROS DE ENERGÍA ===")
                df_retiros = self.data_loader.load_retiros_energia(archivos_config['retiros_energia'])
                if df_retiros is not None:
                    count = self.data_processor.process_retiros_energia(df_retiros)
                    total_registros += count
                    self.logger.info(f"Retiros de energía migrados: {count} registros")
            
            # Migrar Contratos Físicos
            if archivos_config.get('contratos_fisicos'):
                self.logger.info("=== MIGRANDO CONTRATOS FÍSICOS ===")
                df_contratos = self.data_loader.load_contratos_fisicos(archivos_config['contratos_fisicos'])
                if df_contratos is not None:
                    count = self.data_processor.process_contratos_fisicos(df_contratos)
                    total_registros += count
                    self.logger.info(f"Contratos físicos migrados: {count} registros")
            
            self.logger.info(f"=== MIGRACIÓN COMPLETADA ===")
            self.logger.info(f"Total de registros migrados: {total_registros}")
            
        except Exception as e:
            self.logger.error(f"Error durante la migración: {e}")
            raise
        finally:
            self.db_connection.close()

def main():
    """Función principal"""
    
    # Configuración de archivos (ajustar rutas según necesidad)
    archivos_config = {
        'precios_marginales': 'data/cmg2410_15minutal.csv',
        'retiros_energia': 'data/Retiros_2410_15min.csv', 
        'contratos_fisicos': 'data/BBDD Contratos fisicos 2410 def.csv'
    }
    
    # Verificar que los archivos existan
    for tipo, archivo in archivos_config.items():
        if not os.path.exists(archivo):
            print(f"Advertencia: Archivo no encontrado - {archivo}")
            archivos_config[tipo] = None
    
    # Ejecutar migración
    app = MigracionApp()
    app.run_migracion(archivos_config)

if __name__ == "__main__":
    main() 