from dotenv import load_dotenv
import os

load_dotenv()

# Database connection configuration
DB_CONFIG = {
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server'),
    'server': os.getenv('DB_SERVER', 'localhost'),
    'database': os.getenv('DB_NAME', 'master'),
    'username': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'trusted_connection': os.getenv('DB_TRUSTED_CONNECTION', 'yes')
}

def get_connection_string() -> str:
    """Build connection string from configuration"""
    if DB_CONFIG['trusted_connection'].lower() == 'yes':
        return f"mssql+pyodbc://{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver={DB_CONFIG['driver']}&trusted_connection=yes"
    else:
        return f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver={DB_CONFIG['driver']}"