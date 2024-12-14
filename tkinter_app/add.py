from conect import connect_db
from see import display_records
from const import *
from datetime import datetime

def add_record(display_frame, record_type):
    nueva_ventana = tk.Toplevel()
    nueva_ventana.title("Agregar Registros")
    nueva_ventana.geometry(SIZE_SECOND_WINDOW)
    nueva_ventana.config(bg=LIGHT_COLOR)

    inputs = []
    try:
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {record_type} WHERE 1=0")
        headers = [desc[0] for desc in cursor.description]

        #Crear entradas de datos
        for header in headers[:-1]:  #Excluir la última columna donde se almacenaran las fecha
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
            #Incluir la fecha de modificación en los valores
            modification_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            placeholders = ", ".join(["?" for _ in headers])
            query = f"INSERT INTO {record_type} ({', '.join(headers)}) VALUES ({placeholders})"
            cursor.execute(query, values + [modification_date])

            #Insertar datos en otras tablas si es necesario
            if record_type == "table1":
                other_table_query = "INSERT INTO other_table (column1, column2) VALUES (?, ?)"
                cursor.execute(other_table_query, (values[0], values[1]))

            conn.commit()
            nueva_ventana.destroy()
            display_records(display_frame, record_type)
        except Exception as e:
            tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()

    tk.Button(nueva_ventana, text="Agregar", command=submit).pack()
    tk.Button(nueva_ventana, text="Cerrar", command=nueva_ventana.destroy).pack()

    nueva_ventana.protocol("WM_DELETE_WINDOW", lambda: (cursor.close(), conn.close(), nueva_ventana.destroy()))

def add(button_frame, display_frame, types, crear_botones_principales):
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
            command=lambda rt=record_type: add_record(display_frame, rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=lambda: crear_botones_principales(),
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)
