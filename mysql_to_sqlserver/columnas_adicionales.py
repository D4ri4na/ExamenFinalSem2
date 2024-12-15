from config import MSSQL_CONFIG
import pyodbc

# Definir la función parentesis
def parentesis(name, i=0):
    if i == len(name):
        return ""

    if i < len(name) // 2:
        return "(" + name[i] + parentesis(name, i + 1)

    elif i == len(name) // 2:
        if len(name) % 2 == 0:
            return "()" + name[i]+")"+ parentesis(name, i + 1)
        else:
            return "(" + name[i] + ")" + parentesis(name, i + 1)

    else:
        return name[i] + ")" + parentesis(name, i + 1)

# Establecer la conexión
conn = pyodbc.connect( #en vez de declarar todo de nuevo seria mejor llamar a la conexion desde config pero por alguna razon no funciona
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=MSI\\MSSQLSERVER01;"
    "Database=bookstore;"
    "Trusted_Connection=yes;"
)

# Si la conexión es exitosa, procede con las operaciones de la base de datos
cursor = conn.cursor()

# Cambiar el contexto de la base de datos
cursor.execute("USE bookstore")

# Seleccionar los valores de la columna 'name'
cursor.execute("SELECT name FROM genre")
rows = cursor.fetchall()

# Actualizar la columna 'recursiva' con los valores obtenidos de la función parentesis
for row in rows:
    valor_origen = row.name  # Asegúrate de usar el nombre correcto de la columna
    valor_recursiva = parentesis(valor_origen)
    cursor.execute("UPDATE genre SET recursiva = ? WHERE name = ?", valor_recursiva, valor_origen)

# Confirmar los cambios
conn.commit()

# Cerrar la conexión
cursor.close()
conn.close()
