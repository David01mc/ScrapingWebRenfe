"""
Módulo de conexión compartido para Azure SQL Database.
Lee las credenciales desde el fichero .env del directorio del script.
"""

import os
import pyodbc
from pathlib import Path
from dotenv import load_dotenv

# Carga .env desde el mismo directorio que este archivo
load_dotenv(Path(__file__).parent / ".env")

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={os.environ['DB_SERVER']},1433;"
    f"DATABASE={os.environ['DB_NAME']};"
    f"UID={os.environ['DB_USER']};"
    f"PWD={os.environ['DB_PASSWORD']};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)


def get_conn() -> pyodbc.Connection:
    """Devuelve una conexión activa a Azure SQL."""
    return pyodbc.connect(CONN_STR)


def run_ddl(conn: pyodbc.Connection, statements: list[str]) -> None:
    """Ejecuta una lista de sentencias DDL y hace commit."""
    cursor = conn.cursor()
    for sql in statements:
        cursor.execute(sql)
    conn.commit()
