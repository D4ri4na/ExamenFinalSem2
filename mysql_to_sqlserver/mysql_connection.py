import mysql.connector
from mysql.connector import Error
from config import MYSQL_CONFIG

class MySQLConnection:
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)
            if self.connection.is_connected():
                print('Connected to MySQL database')
        except Error as e:
            print(f'Error: {e}')

    def close(self):
        if self.connection:
            self.connection.close()
            print('MySQL connection closed')

    def execute_query(self, query):
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f'Error: {e}')
            return None

    def execute_update(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
        except Error as e:
            print(f'Error: {e}')

#test = MySQLConnection()
#test.connect()

