from conect import connect_db
from const import *
from see import display_records
from datetime import datetime

def mod_record(display_frame, record_type):
    nueva_ventana = tk.Toplevel()
    nueva_ventana.title(f"Modificar {record_type}")
    nueva_ventana.geometry(SIZE_SECOND_WINDOW)
    nueva_ventana.config(bg=LIGHT_COLOR)

    tk.Label(nueva_ventana, text=f"Ingrese el valor de la clave primaria para modificar:", bg=LIGHT_COLOR).pack()
    primary_key_entry = tk.Entry(nueva_ventana)
    primary_key_entry.pack()

    inputs = []
    try:
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {record_type} WHERE 1=0")
        headers = [desc[0] for desc in cursor.description]

        #Crear entradas de datos
        for header in headers[1:-1]:  #Excluir la primera columna (clave primaria) y la última columna (modification)
            tk.Label(nueva_ventana, text=f"{header}:", bg=LIGHT_COLOR).pack()
            entry = tk.Entry(nueva_ventana)
            entry.pack()
            inputs.append(entry)

    except Exception as e:
        tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()
        return

    def load_record():
        try:
            pk_value = primary_key_entry.get().strip()
            if not pk_value:
                raise ValueError("El valor de la clave primaria no puede estar vacío.")

            cursor.execute(f"SELECT * FROM {record_type} WHERE {headers[0]} = ?", (pk_value,))
            current_data = cursor.fetchone()

            if not current_data:
                raise ValueError(f"No se encontró ningún registro con {headers[0]} = {pk_value}")

            for entry, value in zip(inputs, current_data[1:-1]):
                entry.delete(0, tk.END)
                entry.insert(0, value)

        except Exception as e:
            tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()

    def submit():
        try:
            values = [entry.get().strip() for entry in inputs]
            if any(value == "" for value in values):
                raise ValueError("Todos los campos son obligatorios.")

            # Incluir la fecha de modificación en los valores
            modification_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            values.append(modification_date)

            set_clause = ", ".join([f"{header} = ?" for header in headers[1:-1]])
            query = f"UPDATE {record_type} SET {set_clause}, modificacion = ? WHERE {headers[0]} = ?"

            with connect_db() as conn:
                cursor = conn.cursor()
                cursor.execute(query, values + [primary_key_entry.get().strip()])
                conn.commit()

            nueva_ventana.destroy()
            display_records(display_frame, record_type)
        except Exception as e:
            tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()

    tk.Button(nueva_ventana, text="Cargar Registro", command=load_record).pack()
    tk.Button(nueva_ventana, text="Modificar ", command=submit).pack()
    tk.Button(nueva_ventana, text="Cerrar", command=nueva_ventana.destroy).pack()

    nueva_ventana.protocol(
        "WM_DELETE_WINDOW",
        lambda: nueva_ventana.destroy()
    )

def mod(button_frame, display_frame, types, crear_botones_principales):
    for widget in button_frame.winfo_children():
        widget.destroy()

    for record_type in types:
        tk.Button(button_frame,text=record_type,width=BUTTON_WIDTH,bg=LIGHT_COLOR,fg=DARK_COLOR,font=FONT,command=lambda rt=record_type: mod_record(display_frame, rt)
        ).pack(pady=10)

    tk.Button(button_frame,text="Volver",command=lambda: crear_botones_principales(),bg=DARK_COLOR,fg=LIGHT_COLOR,font=FONT
    ).pack(pady=10)
