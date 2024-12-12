from mysql_connection import MySQLConnection
from mssql_connection import MSSQLConnection
from dataclasses import dataclass
from config import MYSQL_CONFIG

@dataclass
class Table:
    name: str
    columns: list
    data: list

# Mapping of MySQL data types to SQL Server data types
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

def migrate_tables_and_data():
    mysql_conn = MySQLConnection()
    mssql_conn = MSSQLConnection()

    try:
        mysql_conn.connect()
        mssql_conn.connect()

        db_name = MYSQL_CONFIG['database']

        # Check if the database already exists
        check_db_query = f"SELECT * FROM sys.databases WHERE name = '{db_name}';"
        db_exists = mssql_conn.execute_query(check_db_query)

        if not db_exists:
            create_db_query = f"CREATE DATABASE {db_name};"
            mssql_conn.execute_update(create_db_query)
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")

        # Cambiar el contexto a la base de datos recién creada
        use_db_query = f"USE {db_name};"
        mssql_conn.execute_update(use_db_query)

        # Obtener nombres de tablas de MySQL
        tables = mysql_conn.execute_query("SHOW TABLES;")
        print(f"Tables retrieved from MySQL: {tables}")  # Debug print statement

        for table in tables:
            table_name = table['Tables_in_' + db_name]
            print(f'Migrando tabla: {table_name}')

            # Verificar si la tabla existe en MS SQL Server
            table_exists_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}';"
            table_exists = mssql_conn.execute_query(table_exists_query)

            if not table_exists:
                # Obtener esquema de la tabla de MySQL
                schema_query = f"SHOW COLUMNS FROM {table_name};"
                columns = mysql_conn.execute_query(schema_query)

                primary_keys = []

                # Crear tabla en MS SQL Server con tipos de datos corregidos
                create_table_query = f"CREATE TABLE {table_name} ("
                for column in columns:
                    col_name = column['Field']
                    col_type = column['Type']

                    # Verificar si la columna es clave primaria
                    if column['Key'] == 'PRI':
                        primary_keys.append(col_name)

                    # Map MySQL data type to SQL Server data type
                    col_type_parts = col_type.split('(')
                    base_type = col_type_parts[0]
                    mapped_type = data_type_mapping.get(base_type, 'VARCHAR')

                    if len(col_type_parts) > 1:
                        # Handle types with length specification
                        length = col_type_parts[1].replace(')', '')
                        mapped_type += f"({length})"

                    create_table_query += f"{col_name} {mapped_type}, "
                
                if primary_keys:
                    create_table_query += f"PRIMARY KEY ({', '.join(primary_keys)}), "
                create_table_query = create_table_query.rstrip(', ') + ");"
                mssql_conn.execute_update(create_table_query)

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

    except Exception as e:
        print(f'Error durante la migración: {e}')
    finally:
        mysql_conn.close()
        mssql_conn.close()

def add_foreign_keys():
    mysql_conn = MySQLConnection()
    mssql_conn = MSSQLConnection()

    try:
        mysql_conn.connect()
        mssql_conn.connect()

        db_name = MYSQL_CONFIG['database']
        use_db_query = f"USE {db_name};"
        mssql_conn.execute_update(use_db_query)

        # Obtener nombres de tablas de MySQL
        tables = mysql_conn.execute_query("SHOW TABLES;")

        for table in tables:
            table_name = table['Tables_in_' + db_name]

            # Obtener claves foráneas de MySQL
            fk_query = f"""
                SELECT
                    kcu.constraint_name,
                    kcu.table_name,
                    kcu.column_name,
                    kcu.referenced_table_name,
                    kcu.referenced_column_name
                FROM
                    information_schema.KEY_COLUMN_USAGE AS kcu
                WHERE
                    kcu.referenced_table_name IS NOT NULL
                    AND kcu.table_name = '{table_name}';
            """
            foreign_keys = mysql_conn.execute_query(fk_query)

            # Añadir claves foráneas en SQL Server
            for fk in foreign_keys:
                constraint_name = fk['constraint_name']
                column_name = fk['column_name']
                referenced_table_name = fk['referenced_table_name']
                referenced_column_name = fk['referenced_column_name']

                print(f"Attempting to add foreign key constraint '{constraint_name}' on table '{table_name}':")
                print(f"Column: {column_name} references {referenced_table_name}({referenced_column_name})")

                # Check if the table exists before adding the constraint
                check_table_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}';"
                table_exists = mssql_conn.execute_query(check_table_query)

                if not table_exists:
                    print(f"Table '{table_name}' does not exist in SQL Server. Skipping foreign key constraint.")
                    continue

                # Validate foreign key references
                validate_fk_query = f"""
                    SELECT COUNT(*) FROM {table_name} AS f
                    LEFT JOIN {referenced_table_name} AS r
                    ON f.{column_name} = r.{referenced_column_name}
                    WHERE r.{referenced_column_name} IS NULL;
                """
                invalid_fk_count = mssql_conn.execute_query(validate_fk_query)[0]['COUNT(*)']

                if invalid_fk_count == 0:
                    alter_table_query = f"""
                        ALTER TABLE {table_name}
                        ADD CONSTRAINT {constraint_name}
                        FOREIGN KEY ({column_name})
                        REFERENCES {referenced_table_name} ({referenced_column_name});
                    """
                    try:
                        mssql_conn.execute_update(alter_table_query)
                        print(f"Foreign key constraint '{constraint_name}' added successfully.")
                    except Exception as e:
                        print(f"Error adding foreign key constraint '{constraint_name}': {e}")
                else:
                    print(f"Cannot add foreign key constraint '{constraint_name}' due to invalid references.")

    except Exception as e:
        print(f'Error durante la migración de claves foráneas: {e}')
    finally:
        mysql_conn.close()
        mssql_conn.close()

if __name__ == "__main__":
    migrate_tables_and_data()
    add_foreign_keys()
