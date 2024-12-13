import pyodbc
from config import MSSQL_CONFIG, MYSQL_CONFIG

class MSSQLConnection:
    """
    Clase para manejar la conexión a una base de datos MS SQL Server.
    """

    def __init__(self):
        """
        Inicializa una nueva instancia de la clase MSSQLConnection.
        """
        self.connection = None

    def connect(self):
        """
        Establece una conexión a la base de datos MS SQL Server.
        Si la base de datos especificada no existe, la crea.
        """
        try:
            # Conectar a la base de datos maestra para crear una nueva base de datos
            self.connection = pyodbc.connect(
                f"DRIVER={{{MSSQL_CONFIG['driver']}}}; "
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE=master;"
                f"Trusted_Connection={MSSQL_CONFIG['trusted_connection']};"
            )
            print('Conectado a la base de datos maestra de MS SQL Server')

            # Crear la base de datos si no existe
            database_name = MYSQL_CONFIG['database']
            create_db_query = f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{database_name}')
            BEGIN
                CREATE DATABASE [{database_name}];
            END;
            """

            # Usar una conexión separada para crear la base de datos
            with pyodbc.connect(
                f"DRIVER={{{MSSQL_CONFIG['driver']}}}; "
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE=master;"
                f"Trusted_Connection={MSSQL_CONFIG['trusted_connection']};",
                autocommit=True  # Establecer autocommit en True
            ) as temp_connection:
                cursor = temp_connection.cursor()
                cursor.execute(create_db_query)
                # print(f'Base de datos {database_name} creada o ya existe')

            # Reconectar a la base de datos recién creada o existente
            self.connection = pyodbc.connect(
                f"DRIVER={{{MSSQL_CONFIG['driver']}}}; "
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE={database_name};"
                f"Trusted_Connection={MSSQL_CONFIG['trusted_connection']};"
            )
            print(f'Conectado a la base de datos {database_name} de MS SQL Server')

        except pyodbc.Error as e:
            print(f'Error: {e}')

    def close(self):
        """
        Cierra la conexión a la base de datos MS SQL Server.
        """
        if self.connection:
            self.connection.close()
            print('Conexión a MS SQL Server cerrada')

    def execute_query(self, query):
        """
        Ejecuta una consulta SQL y devuelve los resultados.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except pyodbc.Error as e:
            print(f'Error: {e}')
            return None

    def execute_update(self, query):
        """
        Ejecuta una consulta SQL de actualización (INSERT, UPDATE, DELETE).
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
        except pyodbc.Error as e:
            print(f'Error: {e}')

