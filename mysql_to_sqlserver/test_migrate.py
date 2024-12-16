from mysql_connection import MySQLConnection
from mssql_connection import MSSQLConnection
from config import MYSQL_CONFIG

def test_migrate_data():
    mysql_conn = MySQLConnection() 
    mssql_conn = MSSQLConnection()

    try:
        mysql_conn.connect()
        mssql_conn.connect()

        db_name = MYSQL_CONFIG['database']
        use_db_query = f"USE {db_name};"
        mssql_conn.execute_update(use_db_query)

        # Obtener nombres de tablas de MySQL
        tables_mysql = mysql_conn.execute_query("SHOW TABLES;")
        tables_sqlserver = mssql_conn.execute_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")

        # Variables para almacenar el número total de registros
        total_records_mysql = 0
        total_records_sqlserver = 0

        # Verificar la integridad de los datos entre cada par de tablas en MySQL y MS SQL Server
        for table in tables_mysql:
            table_name = table['Tables_in_' + db_name]
            print(f'Verificando tabla: {table_name}')

            # Obtener datos de MySQL
            data_query = f"SELECT * FROM {table_name};"
            data_mysql = mysql_conn.execute_query(data_query)
            total_records_mysql += len(data_mysql)

            # Obtener datos de MS SQL Server
            data_query = f"SELECT * FROM {table_name};"
            data_sqlserver = mssql_conn.execute_query(data_query)
            total_records_sqlserver += len(data_sqlserver)

            assert total_records_mysql == total_records_sqlserver, f"El número  de registros no coincide: {total_records_mysql} (MySQL) vs {total_records_sqlserver} (MS SQL Server)"
            print(f"Número de registros en MySQL: {total_records_mysql}")
            print(f"Número de registros en MS SQL Server: {total_records_sqlserver}")
        # Verificar el número total de registros
        assert total_records_mysql == total_records_sqlserver, f"El número total de registros no coincide: {total_records_mysql} (MySQL) vs {total_records_sqlserver} (MS SQL Server)"

        print(f"Número total de registros en MySQL: {total_records_mysql}")
        print(f"Número total de registros en MS SQL Server: {total_records_sqlserver}")
        print("Todas las pruebas pasaron exitosamente.")
    except Exception as e:
        print(f'Error durante la prueba: {e}')
    finally:
        mysql_conn.close()
        mssql_conn.close()
