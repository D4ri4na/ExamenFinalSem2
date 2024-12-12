import tkinter as tk
import pyodbc

#COSAS A ARREGLAR
#el add que se relacione en lkas demas columnas de lass demas tablas  y que se agregue la fecha en donde se hizo la modificaion
# el modifier plus preguntar y el delete

# Tamaño de la ventana principal
SIZE_WINDOW = "1200x600"
SIZE_SECOND_WINDOW = "300x600"
BUTTON_DIRECTION = {'pady': 10, 'anchor': 'e'}
BUTTON_WIDTH = 20  # Ancho fijo para todos los botones
FONT = ("Courier", 14)
DARK_COLOR = "#1e0c42"
LIGHT_COLOR = "#69d2e7"
types = ["Book", "Genre", "Author", "Publisher", "Sales_Order", "City", "Seller", "Customer"]

def connect_db():
    conn = pyodbc.connect(#Para SQLServer
                          "Driver={ODBC Driver 17 for SQL Server};"
                          "Server=MSI\\MSSQLSERVER01;"
                          "Database=bookstore;"
                          "Trusted_Connection=yes;"
                          #Para Mysql
                          #'host': 'localhost','user': 'root','password': '123456789dD.','database': 'bookstore'
                          )
    return conn

def display_records(record_type):
    for widget in display_frame.winfo_children():
        widget.destroy()
    
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {record_type}")
        records = cursor.fetchall()

        headers = [desc[0] for desc in cursor.description]
        header_frame = tk.Frame(display_frame, bg=LIGHT_COLOR)
        header_frame.pack(fill=tk.X)
        for header in headers:
            tk.Label(header_frame, text=header, bg=LIGHT_COLOR, fg=DARK_COLOR, font=FONT, width=15).pack(side=tk.LEFT, padx=5)

        for record in records:
            row_frame = tk.Frame(display_frame, bg=LIGHT_COLOR)
            row_frame.pack(fill=tk.X)
            for value in record:
                tk.Label(row_frame, text=str(value), bg=LIGHT_COLOR, fg=DARK_COLOR, font=FONT, width=15).pack(side=tk.LEFT, padx=5)

    except Exception as e:
        tk.Label(display_frame, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red", font=FONT).pack(pady=5)

    cursor.close()
    conn.close()

def see():
    for widget in button_frame.winfo_children():
        widget.destroy()

    for record_type in types:
        tk.Button(
            button_frame,
            text=record_type,
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT,
            command=lambda rt=record_type: display_records(rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=crear_botones_principales,
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)

def add_record(record_type):
    nueva_ventana = tk.Toplevel()
    nueva_ventana.title("Agregar Registros")
    nueva_ventana.geometry(SIZE_SECOND_WINDOW)
    nueva_ventana.config(bg=LIGHT_COLOR)

    inputs = []
    try:
        conn = connect_db()
        cursor = conn.cursor()

        # Obtener nombres de las columnas
        cursor.execute(f"SELECT * FROM {record_type} WHERE 1=0")  # Solo devuelve la estructura
        headers = [desc[0] for desc in cursor.description]

        # Crear etiquetas y entradas para cada columna
        for header in headers:
            tk.Label(nueva_ventana, text=f"{header}:", bg=LIGHT_COLOR).pack()
            entry = tk.Entry(nueva_ventana)
            entry.pack()
            inputs.append(entry)

    except Exception as e:
        tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()
        return

    def submit():
        values = [entry.get() for entry in inputs]
        try:
            # Generar la consulta con placeholders
            placeholders = ", ".join(["?" for _ in values])
            query = f"INSERT INTO {record_type} VALUES ({placeholders})"
            cursor.execute(query, values)
            conn.commit()
            nueva_ventana.destroy()
            display_records(record_type)
        except Exception as e:
            tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()

    tk.Button(nueva_ventana, text="Agregar", command=submit).pack()
    tk.Button(nueva_ventana, text="Cerrar", command=nueva_ventana.destroy).pack()

    # Cerrar recursos al salir
    nueva_ventana.protocol("WM_DELETE_WINDOW", lambda: (cursor.close(), conn.close(), nueva_ventana.destroy()))


def add():
    for widget in button_frame.winfo_children():
        widget.destroy()

    for record_type in types:
        tk.Button(
            button_frame,
            text=record_type,
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT,
            command=lambda rt=record_type: add_record(rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=crear_botones_principales,
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)

def mod_record(record_type):
    nueva_ventana = tk.Toplevel()
    nueva_ventana.title(f"Modificar {record_type}")
    nueva_ventana.geometry(SIZE_SECOND_WINDOW)
    nueva_ventana.config(bg=LIGHT_COLOR)

    inputs = []
    try:
        with connect_db() as conn:
            cursor = conn.cursor()

            # Obtener nombres de las columnas
            cursor.execute(f"SELECT * FROM {record_type} WHERE 1=0")  # Solo devuelve la estructura
            headers = [desc[0] for desc in cursor.description]

            # Crear etiquetas y entradas para cada columna
            for header in headers:
                tk.Label(nueva_ventana, text=f"{header}:", bg=LIGHT_COLOR).pack()
                entry = tk.Entry(nueva_ventana)
                entry.pack()
                inputs.append((header, entry))
    except Exception as e:
        tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()
        return

    def submit():
        try:
            # Validar entradas
            values = [entry.get().strip() for _, entry in inputs]
            if any(value == "" for value in values):
                raise ValueError("Todos los campos son obligatorios.")

            # Generar la consulta con placeholders
            placeholders = ", ".join(["?" for _ in values])
            query = f"UPDATE {record_type} SET {placeholders} WHERE id = ?"

            with connect_db() as conn:
                cursor = conn.cursor()
                cursor.execute(query, values)
                conn.commit()
                
            nueva_ventana.destroy()
            display_records(record_type)
        except Exception as e:
            tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()

    tk.Button(nueva_ventana, text="Agregar", command=submit).pack()
    tk.Button(nueva_ventana, text="Cerrar", command=nueva_ventana.destroy).pack()

    nueva_ventana.protocol(
        "WM_DELETE_WINDOW", 
        lambda: nueva_ventana.destroy()
    )


def mod():
    for widget in button_frame.winfo_children():
        widget.destroy()

    for record_type in types:
        tk.Button(
            button_frame,
            text=record_type,
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT,
            command=lambda rt=record_type: mod_record(rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=crear_botones_principales,
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)

def delete_record(record_type):
    def delete_entry():
        primary_key = entry.get()
        if not primary_key:
            tk.Label(delete_window, text="Por favor ingrese un valor.", bg=LIGHT_COLOR, fg="red", font=FONT).pack(pady=5)
            return

        try:
            conn = connect_db()
            cursor = conn.cursor()
            # Asegúrate de cambiar "id" al nombre real de la clave primaria
            cursor.execute(f"DELETE FROM {record_type} WHERE id = ?", (primary_key,))
            conn.commit()
            cursor.close()
            conn.close()
            delete_window.destroy()
            display_records(record_type)  # Actualiza la visualización después de eliminar
        except Exception as e:
            tk.Label(delete_window, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red", font=FONT).pack(pady=5)

    delete_window = tk.Toplevel()
    delete_window.title(f"Eliminar de {record_type}")
    delete_window.geometry(SIZE_SECOND_WINDOW)
    delete_window.config(bg=LIGHT_COLOR)

    tk.Label(delete_window, text="Ingrese la clave primaria:", bg=LIGHT_COLOR, fg=DARK_COLOR).pack(pady=10)
    entry = tk.Entry(delete_window, width=30)
    entry.pack(pady=10)

    tk.Button(delete_window, text="Eliminar", command=delete_entry, bg=DARK_COLOR, fg=LIGHT_COLOR).pack(pady=10)
    tk.Button(delete_window, text="Cerrar", command=delete_window.destroy, bg=DARK_COLOR, fg=LIGHT_COLOR).pack(pady=10)

def delete():
    for widget in button_frame.winfo_children():
        widget.destroy()

    for record_type in types:
        tk.Button(
            button_frame,
            text=record_type,
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT,
            command=lambda rt=record_type: delete_record(rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=crear_botones_principales,
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)
    

def crear_botones_principales():
    # Limpiar los botones actuales
    for widget in button_frame.winfo_children():
        widget.destroy()

    tk.Label(button_frame, text="Gestion de Datos", bg=DARK_COLOR, fg=LIGHT_COLOR, font=FONT).pack(BUTTON_DIRECTION)

    tk.Button(
        button_frame,
        text="Visualizar registros",
        command=see,
        width=BUTTON_WIDTH,
        bg=LIGHT_COLOR,
        fg=DARK_COLOR,
        font=FONT
    ).pack(BUTTON_DIRECTION)

    tk.Button(
        button_frame,
        text="Agregar registros",
        command=add,
        width=BUTTON_WIDTH,
        bg=LIGHT_COLOR,
        fg=DARK_COLOR,
        font=FONT
    ).pack(BUTTON_DIRECTION)

    tk.Button(
        button_frame,
        text="Modificar registros",
        command=mod,
        width=BUTTON_WIDTH,
        bg=LIGHT_COLOR,
        fg=DARK_COLOR,
        font=FONT
    ).pack(BUTTON_DIRECTION)

    tk.Button(
        button_frame,
        text="Eliminar registros",
        command=delete,
        width=BUTTON_WIDTH,
        bg=LIGHT_COLOR,
        fg=DARK_COLOR,
        font=FONT
    ).pack(BUTTON_DIRECTION)

# Inicializar aplicación
app = tk.Tk()
app.geometry(SIZE_WINDOW)
app.title("Gestor De Datos")
app.configure(bg=DARK_COLOR)

button_frame = tk.Frame(app, bg=DARK_COLOR)
button_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=10, pady=10)

display_frame = tk.Frame(app, bg=LIGHT_COLOR)
display_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

crear_botones_principales()  # Cargar los botones principales al iniciar

app.mainloop()
