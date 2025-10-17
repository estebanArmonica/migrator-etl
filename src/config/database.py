import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging

load_dotenv()

class DatabaseConfig:
    """Configuración de la base de datos"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        
    def get_connection_string(self):
        """Retorna la cadena de conexión"""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"

class DatabaseConnection:
    """Manejador de conexión a la base de datos"""
    
    def __init__(self):
        self.config = DatabaseConfig()
        self._connection = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            self._connection = psycopg2.connect(
                self.config.get_connection_string(),
                cursor_factory=RealDictCursor
            )
            self.logger.info("Conexión a la base de datos establecida")
        except Exception as e:
            self.logger.error(f"Error conectando a la base de datos: {e}")
            raise
    
    def close(self):
        """Cierra la conexión"""
        if self._connection:
            self._connection.close()
            self.logger.info("Conexión a la base de datos cerrada")
    
    def get_connection(self):
        """Retorna la conexión activa"""
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection