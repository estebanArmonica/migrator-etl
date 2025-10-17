import logging
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.config.database import DatabaseConnection
from src.models.repositories import DataRepository
from src.services.data_loader import SimpleDataLoader
from src.services.data_processor import SimpleDataProcessor
from src.utils.logger import setup_logger

class SimpleMigracionApp:
    """Aplicación simplificada de migración de datos"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.db_connection = DatabaseConnection()
        self.data_repository = DataRepository(self.db_connection)
        self.data_loader = SimpleDataLoader()
        self.data_processor = SimpleDataProcessor(self.data_repository)
    
    def run_migracion(self, archivos_config: dict):
        """Ejecuta el proceso completo de migración"""
        self.logger.info("Iniciando proceso de migración simplificado...")
        
        total_registros = 0
        
        try:
            # Migrar Precios Marginales
            if archivos_config.get('precios_marginales'):
                self.logger.info("=== MIGRANDO PRECIOS MARGINALES ===")
                data_precios = self.data_loader.load_precios_marginales(archivos_config['precios_marginales'])
                if data_precios:
                    count = self.data_processor.process_precios_marginales(data_precios)
                    total_registros += count
                    self.logger.info(f"Precios marginales migrados: {count} registros")
            
            # Migrar Retiros de Energía
            if archivos_config.get('retiros_energia'):
                self.logger.info("=== MIGRANDO RETIROS DE ENERGÍA ===")
                data_retiros = self.data_loader.load_retiros_energia(archivos_config['retiros_energia'])
                if data_retiros:
                    count = self.data_processor.process_retiros_energia(data_retiros)
                    total_registros += count
                    self.logger.info(f"Retiros de energía migrados: {count} registros")
            
            # Migrar Contratos Físicos
            if archivos_config.get('contratos_fisicos'):
                self.logger.info("=== MIGRANDO CONTRATOS FÍSICOS ===")
                data_contratos = self.data_loader.load_contratos_fisicos(archivos_config['contratos_fisicos'])
                if data_contratos:
                    count = self.data_processor.process_contratos_fisicos(data_contratos)
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
    """Función principal simplificada"""
    
    # Configuración de archivos
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
    app = SimpleMigracionApp()
    app.run_migracion(archivos_config)

if __name__ == "__main__":
    main()
