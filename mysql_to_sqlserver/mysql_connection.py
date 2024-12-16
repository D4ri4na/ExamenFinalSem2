import mysql.connector
from mysql.connector import Error
from config import MYSQL_CONFIG

class MySQLConnection:
    """
    Clase para manejar la conexión a una base de datos MySQL.
    """

    def __init__(self):
        """
        Inicializa una nueva instancia de la clase MySQLConnection.
        """
        self.connection = None

    def connect(self):
        """
        Establece una conexión a la base de datos MySQL utilizando las configuraciones especificadas.
        """
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)

        except Error as e:
            print(f'Error: {e}')

    def close(self):
        """
        Cierra la conexión a la base de datos MySQL.
        """
        if self.connection:
            self.connection.close()

    def execute_query(self, query):
        """
        Ejecuta una consulta SQL y devuelve los resultados.
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
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
        except Error as e:
            print(f'Error: {e}')


#test = MySQLConnection()
#test.connect()
