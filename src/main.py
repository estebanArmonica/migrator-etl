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
    
   # En tu función main o donde procesas los retiros
    def run_migracion(self, archivos_config):
        try:
            self.logger.info("Iniciando proceso de migración simplificado...")
            
            self.logger.info("=== MIGRANDO PRECIOS MARGINALES ===")
            data_precios = self.data_loader.load_precios_marginales(archivos_config['precios_marginales'])
            self.logger.info(f"Precios marginales cargados: {len(data_precios)} registros")
            
            if data_precios:
                count_precios = self.data_processor.process_precios_marginales(data_precios)
                self.logger.info(f"Precios marginales migrados: {count_precios} registros")
            
            self.logger.info("=== MIGRANDO RETIROS DE ENERGÍA ===")
            # Agregar timing
            start_time = datetime.now()
            data_retiros = self.data_loader.load_retiros_energia(archivos_config['retiros_energia'])
            load_time = datetime.now() - start_time
            self.logger.info(f"Retiros de energía cargados en {load_time}: {len(data_retiros)} registros")
            
            if data_retiros:
                start_time = datetime.now()
                count_retiros = self.data_processor.process_retiros_energia(data_retiros)
                process_time = datetime.now() - start_time
                self.logger.info(f"Retiros de energía migrados en {process_time}: {count_retiros} registros")
            
        except Exception as e:
            self.logger.error(f"Error en el proceso de migración: {e}")
            raise
    
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
