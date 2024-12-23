from mysql_connection import MySQLConnection
from mssql_connection import MSSQLConnection
from config import MYSQL_CONFIG
from recursion import parentesis
from test_migrate  import test_migrate_data
from datetime import date

# Map de tipos de datos de MySQL a tipos de datos de SQL Server
data_type_mapping = {
    'int': 'INT',
    'tinyint': 'TINYINT',
    'smallint': 'SMALLINT',
    'mediumint': 'INT',
    'bigint': 'BIGINT',
    'float': 'FLOAT',
    'double': 'FLOAT',
    'decimal': 'DECIMAL',
    'numeric': 'NUMERIC',
    'char': 'CHAR',
    'varchar': 'VARCHAR',
    'binary': 'BINARY',
    'varbinary': 'VARBINARY',
    'tinyblob': 'VARBINARY',
    'blob': 'VARBINARY',
    'mediumblob': 'VARBINARY',
    'longblob': 'VARBINARY',
    'tinytext': 'TEXT',
    'text': 'TEXT',
    'mediumtext': 'TEXT',
    'longtext': 'TEXT',
    'date': 'DATE',
    'datetime': 'DATETIME',
    'timestamp': 'DATETIME',
    'time': 'TIME',
    'year': 'INT',
    'enum': 'VARCHAR',
    'set': 'VARCHAR',
    'bit': 'BIT',
    'bool': 'BIT',
    'boolean': 'BIT'
}

def migrate_data():
    mysql_conn = MySQLConnection()
    mssql_conn = MSSQLConnection()

    try:
        mysql_conn.connect()
        mssql_conn.connect()

        db_name = MYSQL_CONFIG['database']
        use_db_query = f"USE {db_name};"
        mssql_conn.execute_update(use_db_query)

        # Verificar si la base de datos MS SQL Server está vacía
        check_empty_db_query = "SELECT COUNT(*) AS TotalTablas FROM INFORMATION_SCHEMA.TABLES;"
        result = mssql_conn.execute_query(check_empty_db_query)
        tables_db = result[0]
        # print(f" the result: {tables_db}")

        tables_mysql = mysql_conn.execute_query("SHOW TABLES;")
        tables_sqlserver = mssql_conn.execute_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")


        if tables_db[0] == 0: 
            # Obtener nombres de tablas de MySQL
            #tables = mysql_conn.execute_query("SHOW TABLES;")
            #print(f"Tablas recuperadas de MySQL: {tables}")

            foreign_keys_list = []

            # Iniciar transacción
            mssql_conn.execute_update("BEGIN TRANSACTION;")

            try:
                for table in tables_mysql:
                    table_name = table['Tables_in_' + db_name]
                    print(f'Migrando tabla: {table_name}')

                    # Verificar si la tabla existe en MS SQL Server
                    table_exists_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}';"
                    table_exists = mssql_conn.execute_query(table_exists_query)

                    if not table_exists:
                        # Obtener esquema de tabla de MySQL
                        schema_query = f"SHOW COLUMNS FROM {table_name};"
                        columns = mysql_conn.execute_query(schema_query)

                        # Obtener información de clave primaria
                        primary_key_query = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY';"
                        primary_keys = mysql_conn.execute_query(primary_key_query)

                        # Obtener información de clave foránea
                        foreign_key_query = f"""
                            SELECT
                                kcu.constraint_name,
                                kcu.table_name,
                                kcu.column_name,
                                kcu.referenced_table_name,
                                kcu.referenced_column_name
                            FROM
                                information_schema.table_constraints AS tc
                                JOIN information_schema.key_column_usage AS kcu
                                ON tc.constraint_name = kcu.constraint_name
                                AND tc.table_schema = kcu.table_schema
                            WHERE
                                tc.constraint_type = 'FOREIGN KEY' AND
                                tc.table_name = '{table_name}';
                        """
                        foreign_keys = mysql_conn.execute_query(foreign_key_query)

                        # Verificar si la tabla tiene claves foráneas
                        if foreign_keys:
                            foreign_keys_list.extend(foreign_keys)

                        # Crear tabla en MS SQL Server con tipos de datos corregidos
                        create_table_query = f"CREATE TABLE {table_name} ("
                        for column in columns:
                            col_name = column['Field']
                            col_type = column['Type']

                            # Mapear tipo de datos de MySQL a tipo de datos de SQL Server
                            col_type_parts = col_type.split('(')
                            base_type = col_type_parts[0]
                            mapped_type = data_type_mapping.get(base_type, 'VARCHAR')

                            if len(col_type_parts) > 1:
                                # Manejar tipos con especificación de longitud
                                length = col_type_parts[1].replace(')', '')
                                mapped_type += f"({length})"

                            create_table_query += f"{col_name} {mapped_type}, "

                        # Agregar restricción de clave primaria
                        if primary_keys:
                            primary_key_columns = [key['Column_name'] for key in primary_keys]
                            primary_key_constraint = f"PRIMARY KEY ({', '.join(primary_key_columns)})"
                            create_table_query += primary_key_constraint

                        create_table_query = create_table_query.rstrip(', ') + ");"
                        mssql_conn.execute_update(create_table_query)

            except Exception as e:
                # Revertir transacción en caso de error
                mssql_conn.execute_update("ROLLBACK;")
                print(f"Error durante la creación de la tabla o la adición de claves foráneas: {e}")
                return

            # Insertar datos en las tablas
            for table in tables_mysql:
                table_name = table['Tables_in_' + db_name]
                print(f'Insertando datos en la tabla: {table_name}')

                # Obtener datos de MySQL
                data_query = f"SELECT * FROM {table_name};"
                data = mysql_conn.execute_query(data_query)

                # Insertar datos en MS SQL Server
                for row in data:
                    insert_query = f"INSERT INTO {table_name} VALUES ("
                    for value in row.values():
                        if isinstance(value, str):
                            insert_query += f"'{value.replace("'", "''")}', "  # Escapar comillas simples
                        else:
                            insert_query += f"'{value}', "
                    insert_query = insert_query.rstrip(', ') + ");"
                    try:
                        mssql_conn.execute_update(insert_query)
                    except Exception as e:
                        print(f'Error: {e}')  # Mostrar el error pero continuar con el siguiente registro




            # Agregar restricciones de clave foránea a MS SQL Server
            for fk in foreign_keys_list:
                # Imprimir toda la información de la clave foránea
                #print(f"Información de Clave Foránea: {fk}")

                constraint_name = fk['CONSTRAINT_NAME']
                table_name = fk['TABLE_NAME']
                column_name = fk['COLUMN_NAME']
                referenced_table_name = fk['REFERENCED_TABLE_NAME']
                referenced_column_name = fk['REFERENCED_COLUMN_NAME']

                alter_table_query = f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {constraint_name}
                FOREIGN KEY ({column_name})
                REFERENCES {referenced_table_name} ({referenced_column_name});
                
                """

                try:
                    mssql_conn.execute_update(alter_table_query)
                    print(f"Restricción de clave foránea agregada exitosamente a la tabla '{table_name}'.")
                except Exception as e:
                    print(f"Error al agregar restricción de clave foránea a la tabla '{table_name}': {e}")
        
            test_migrate_data()
        
# DEFENSA DEL PROYECTO - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - -- - - - - - - - -- - - - - -- 

            table_genre = "genre"
            alter_genre_query = f"ALTER TABLE {table_genre} ADD parentesis VARCHAR(255);"
            try:
                mssql_conn.execute_update(alter_genre_query)
                print(f'Columna "parentesis" agregada a la tabla: {table_genre}')
            except Exception as e:
                print(f'Error al agregar columna "parentesis" a la tabla {table_genre}: {e}')            


            # Consulta para seleccionar los valores de name
            select_genre_query = f"SELECT name FROM {table_genre};"
            records = mssql_conn.execute_query(select_genre_query)

            for record in records:
                # Asegúrate de que record sea una cadena
                name_value = str(record[0])  # Suponiendo que record es una tupla y el nombre está en el primer campo
                # print(f"name: {name_value}") verificar si se esta mostrando el valor de la columna name

                valor_modificado = parentesis(name_value)  # Invertir el valor de name

                # Formatear la consulta SQL manualmente
                update_query = f"UPDATE {table_genre} SET parentesis = '{valor_modificado}' WHERE name = '{name_value}'"
                try:
                    mssql_conn.execute_update(update_query)
                    print(f"Registro hecho exitosamente en la tabla genre.")
                except Exception as e:
                    print(f'Error al actualizar valores en la columna "parentesis": {e}')

# FIN DEFENSA DEL PROYECTO  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - -- - - - - - - - -- - - - - -- 


            # Agregar columna "modificación" a cada tabla existente
            for table in tables_mysql:
                table_name = table['Tables_in_' + db_name]
                print(f'Agregando columna "modificación" a la tabla: {table_name}')

                # Verificar si la columna ya existe
                check_column_query = f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'modificacion';
                """
                column_exists = mssql_conn.execute_query(check_column_query)

                if not column_exists:
                    alter_table_query = f"ALTER TABLE {table_name} ADD modificacion VARCHAR(255);"
                    try:
                        mssql_conn.execute_update(alter_table_query)
                        print(f'Columna "modificación" agregada a la tabla: {table_name}')
                    except Exception as e:
                        print(f'Error al agregar columna "modificación" a la tabla {table_name}: {e}')
                
                    # Obtener la fecha actual en el formato DD-MM-YYYY
                current_date = date.today().strftime('%d-%m-%Y')

                # Actualizar todos los registros en la columna "modificacion" con la fecha actual
                update_query = f"UPDATE {table_name} SET modificacion = '{current_date}';"
                try:
                    mssql_conn.execute_update(update_query)
                    print(f'Columna "modificación" actualizada con la fecha actual en la tabla: {table_name}')
                except Exception as e:
                    print(f'Error al actualizar la columna "modificación" en la tabla {table_name}: {e}')


    except Exception as e:
        print(f'Error durante la migración: {e}')
    finally:
        mysql_conn.close()
        mssql_conn.close()
    

if __name__ == "__main__":
    migrate_data()
