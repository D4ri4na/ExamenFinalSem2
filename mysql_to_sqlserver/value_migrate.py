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

def migrate_data():
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

        # Change context to the newly created database
        use_db_query = f"USE {db_name};"
        mssql_conn.execute_update(use_db_query)

        # Get table names from MySQL
        tables = mysql_conn.execute_query("SHOW TABLES;")
        print(f"Tables retrieved from MySQL: {tables}")  # Debug print statement

        foreign_keys_list = []

        # Start transaction
        mssql_conn.execute_update("BEGIN TRANSACTION;")

        try:
            for table in tables:
                table_name = table['Tables_in_' + db_name]
                print(f'Migrating table: {table_name}')

                # Check if the table exists in MS SQL Server
                table_exists_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}';"
                table_exists = mssql_conn.execute_query(table_exists_query)

                if not table_exists:
                    # Get table schema from MySQL
                    schema_query = f"SHOW COLUMNS FROM {table_name};"
                    columns = mysql_conn.execute_query(schema_query)

                    # Get primary key information
                    primary_key_query = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY';"
                    primary_keys = mysql_conn.execute_query(primary_key_query)

                    # Get foreign key information
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

                    # Check if the table has foreign keys
                    if foreign_keys:
                        foreign_keys_list.extend(foreign_keys)

                    # Create table in MS SQL Server with corrected data types
                    create_table_query = f"CREATE TABLE {table_name} ("
                    for column in columns:
                        col_name = column['Field']
                        col_type = column['Type']

                        # Map MySQL data type to SQL Server data type
                        col_type_parts = col_type.split('(')
                        base_type = col_type_parts[0]
                        mapped_type = data_type_mapping.get(base_type, 'VARCHAR')

                        if len(col_type_parts) > 1:
                            # Handle types with length specification
                            length = col_type_parts[1].replace(')', '')
                            mapped_type += f"({length})"

                        create_table_query += f"{col_name} {mapped_type}, "

                    # Add primary key constraint
                    if primary_keys:
                        primary_key_columns = [key['Column_name'] for key in primary_keys]
                        primary_key_constraint = f"PRIMARY KEY ({', '.join(primary_key_columns)})"
                        create_table_query += primary_key_constraint

                    create_table_query = create_table_query.rstrip(', ') + ");"
                    mssql_conn.execute_update(create_table_query)



        except Exception as e:
            # Rollback transaction in case of error
            mssql_conn.execute_update("ROLLBACK;")
            print(f"Error during table creation or foreign key addition: {e}")
            return
        
        


        # Insert data into the tables
        for table in tables:
            table_name = table['Tables_in_' + db_name]
            print(f'Inserting data into table: {table_name}')

            # Get data from MySQL
            data_query = f"SELECT * FROM {table_name};"
            data = mysql_conn.execute_query(data_query)

            # Insert data into MS SQL Server
            for row in data:
                insert_query = f"INSERT INTO {table_name} VALUES ("
                for value in row.values():
                    if isinstance(value, str):
                        insert_query += f"'{value.replace("'", "''")}', "  # Escape single quotes
                    else:
                        insert_query += f"'{value}', "
                insert_query = insert_query.rstrip(', ') + ");"
                try:
                    mssql_conn.execute_update(insert_query)
                except Exception as e:
                    print(f'Error: {e}')  # Show the error but continue with the next record

         # Add foreign key constraints to MS SQL Server
        for fk in foreign_keys_list:
            # Imprimir toda la información de la clave foránea
            #print(f"Foreign Key Info: {fk}")

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
                print(f"Foreign key constraint added successfully to table '{table_name}'.")
            except Exception as e:
                print(f"Error adding foreign key constraint to table '{table_name}': {e}")

        # Add "modificación" column to each existing table
        for table in tables:
            table_name = table['Tables_in_' + db_name]
            print(f'Adding column "modificación" to table: {table_name}')

            # Check if the column already exists
            check_column_query = f"""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'modificacion';
            """
            column_exists = mssql_conn.execute_query(check_column_query)

            if not column_exists:
                alter_table_query = f"ALTER TABLE {table_name} ADD modificacion DATETIME;"
                try:
                    mssql_conn.execute_update(alter_table_query)
                    print(f'Column "modificación" added to table: {table_name}')
                except Exception as e:
                    print(f'Error adding column "modificación" to table {table_name}: {e}')

    except Exception as e:
        print(f'Error during migration: {e}')
    finally:
        mysql_conn.close()
        mssql_conn.close()

if __name__ == "__main__":
    migrate_data()
