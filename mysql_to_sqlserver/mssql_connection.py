# mssql_connection.py

import pyodbc
from config import MSSQL_CONFIG, MYSQL_CONFIG

class MSSQLConnection:
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            # Connect to the master database to create a new database
            self.connection = pyodbc.connect(
                f"DRIVER={{{MSSQL_CONFIG['driver']}}}; "
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE=master;"
                f"Trusted_Connection={MSSQL_CONFIG['trusted_connection']};"
            )
            print('Connected to MS SQL Server master database')

            # Create the database if it does not exist
            database_name = MYSQL_CONFIG['database']
            create_db_query = f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{database_name}')
            BEGIN
                CREATE DATABASE [{database_name}];
            END;
            """

            # Use a separate connection for creating the database
            with pyodbc.connect(
                f"DRIVER={{{MSSQL_CONFIG['driver']}}}; "
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE=master;"
                f"Trusted_Connection={MSSQL_CONFIG['trusted_connection']};",
                autocommit=True  # Set autocommit to True
            ) as temp_connection:
                cursor = temp_connection.cursor()
                cursor.execute(create_db_query)
                print(f'Database {database_name} created or already exists')

            # Reconnect to the newly created or existing database
            self.connection = pyodbc.connect(
                f"DRIVER={{{MSSQL_CONFIG['driver']}}}; "
                f"SERVER={MSSQL_CONFIG['server']};"
                f"DATABASE={database_name};"
                f"Trusted_Connection={MSSQL_CONFIG['trusted_connection']};"
            )
            print(f'Connected to MS SQL Server database {database_name}')

        except pyodbc.Error as e:
            print(f'Error: {e}')

    def close(self):
        if self.connection:
            self.connection.close()
            print('MS SQL Server connection closed')

    def execute_query(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except pyodbc.Error as e:
            print(f'Error: {e}')
            return None

    def execute_update(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
        except pyodbc.Error as e:
            print(f'Error: {e}')

# Uncomment the following lines to test the connection
# test = MSSQLConnection()
# test.connect()
# test.close()
