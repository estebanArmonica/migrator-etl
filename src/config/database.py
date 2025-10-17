import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    """Configuración de la base de datos usando SQLAlchemy"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        
    def get_connection_string(self):
        """Retorna la cadena de conexión para SQLAlchemy"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class DatabaseConnection:
    """Manejador de conexión a la base de datos usando SQLAlchemy"""
    
    def __init__(self):
        self.config = DatabaseConfig()
        self._engine = None
        self._session_factory = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            connection_string = self.config.get_connection_string()
            self._engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                echo=False  # Cambiar a True para debug
            )
            
            # Probar la conexión
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._session_factory = sessionmaker(bind=self._engine)
            self.logger.info("Conexión a la base de datos establecida")
            
        except Exception as e:
            self.logger.error(f"Error conectando a la base de datos: {e}")
            raise
    
    def close(self):
        """Cierra la conexión"""
        if self._engine:
            self._engine.dispose()
            self.logger.info("Conexión a la base de datos cerrada")
    
    def get_session(self):
        """Retorna una nueva sesión"""
        if not self._session_factory:
            self.connect()
        return self._session_factory()
    
    def get_engine(self):
        """Retorna el engine de SQLAlchemy"""
        if not self._engine:
            self.connect()
        return self._engine

# Función de utilidad para ejecutar consultas
def execute_query(session, query, params=None):
    """Ejecuta una consulta y retorna resultados"""
    try:
        result = session.execute(text(query), params or {})
        if query.strip().upper().startswith('SELECT'):
            return result.fetchall()
        else:
            session.commit()
            return result
    except SQLAlchemyError as e:
        session.rollback()
        raise e
