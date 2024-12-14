import pyodbc

def connect_db():
    conn = pyodbc.connect( #cambiar dependiendo de en que equipo se ejecute
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=MSI\\MSSQLSERVER01;"
        "Database=bookstore;"
        "Trusted_Connection=yes;"
        #Para Mysql
        #'host': 'localhost','user': 'root','password': '123456789dD.','database': 'bookstore'
    )
    return conn
