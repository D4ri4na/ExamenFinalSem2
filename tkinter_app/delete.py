from conect import connect_db
from const import *
from see import display_records

def delete_record(display_frame, record_type):
    nueva_ventana = tk.Toplevel()
    nueva_ventana.title(f"Eliminar {record_type}")
    nueva_ventana.geometry(SIZE_SECOND_WINDOW)
    nueva_ventana.config(bg=LIGHT_COLOR)

    tk.Label(nueva_ventana, text=f"Ingrese el valor de la clave primaria para eliminar:", bg=LIGHT_COLOR).pack()
    primary_key_entry = tk.Entry(nueva_ventana)
    primary_key_entry.pack()

    try:
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {record_type} WHERE 1=0")
        headers = [desc[0] for desc in cursor.description]

    except Exception as e:
        tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()
        return

    def submit():
        try:
            pk_value = primary_key_entry.get().strip()
            if not pk_value:
                raise ValueError("El valor de la clave primaria no puede estar vacío.")

            with connect_db() as conn:
                cursor = conn.cursor()

                related_tables = get_related_tables(record_type)
                for related_table in related_tables:
                    related_query = f"DELETE FROM {related_table} WHERE {headers[0]} = ?"
                    cursor.execute(related_query, (pk_value,))

                query = f"DELETE FROM {record_type} WHERE {headers[0]} = ?"
                cursor.execute(query, (pk_value,))

                conn.commit()

            nueva_ventana.destroy()
            display_records(display_frame, record_type)
        except Exception as e:
            tk.Label(nueva_ventana, text=f"Error: {str(e)}", bg=LIGHT_COLOR, fg="red").pack()

    tk.Button(nueva_ventana, text="Eliminar", command=submit).pack()
    tk.Button(nueva_ventana, text="Cerrar", command=nueva_ventana.destroy).pack()

    nueva_ventana.protocol(
        "WM_DELETE_WINDOW",
        lambda: nueva_ventana.destroy()
    )

def get_related_tables(record_type):
    related_tables = []
    if record_type == 'parent_table':
        related_tables = ['child_table']
    return related_tables

def delete(button_frame, display_frame, types, crear_botones_principales):
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
            command=lambda rt=record_type: delete_record(display_frame, rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=lambda: crear_botones_principales(),
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)
