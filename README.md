# Migrador de Datos - Transferencia Económica

Aplicación para migrar datos desde archivos CSV a la base de datos PostgreSQL.

## Instalación

1. Crear entorno virtual:
```bash
    # Windows
    python -m virtualenv venv
    
    # activamos el entorno
    venv\Scripts\activate     # Windows

    # desactivamos el entorno
    venv\Scripts\deactivate     # Windows
    
    # Linux/Mac
    python3 -m virtualenv venv

    # activamos el entorno
    source venv/bin/activate  # Linux/Mac

    # desactivamos el entorno
    deactivate   # Linux/Mac
```


## Características de la aplicación:

1. **Arquitectura modular**: Separación clara de responsabilidades
2. **Manejo de errores**: Logging robusto y manejo de excepciones
3. **Validación de datos**: Limpieza y validación antes de la inserción
4. **Progreso visual**: Barra de progreso para procesos largos
5. **Reintentos automáticos**: Para inserción de datos
6. **Configuración flexible**: Variables de entorno para conexión DB
7. **Logging completo**: Seguimiento detallado del proceso

Para usar la aplicación, simplemente configura tu archivo `.env` con las credenciales de PostgreSQL y coloca los archivos CSV en la carpeta `data/`. La aplicación se encargará de todo el proceso de migración automáticamente.