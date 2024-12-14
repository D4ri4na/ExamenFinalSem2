import tkinter as tk
from conect import connect_db
from const import *

def display_records(display_frame, record_type):
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

def see(button_frame, display_frame, types, crear_botones_principales):
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
            command=lambda rt=record_type: display_records(display_frame, rt)
        ).pack(pady=10)

    tk.Button(
        button_frame,
        text="Volver",
        command=lambda: crear_botones_principales(),
        bg=DARK_COLOR,
        fg=LIGHT_COLOR,
        font=FONT
    ).pack(pady=10)