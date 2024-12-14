from see import see
from add import add
from mod import mod
from delete import delete
from const import *

class App:
    def __init__(self, root):
        """
        Inicializa la aplicación con la ventana principal y configura los marcos para los botones y la visualización.
        """
        self.root = root
        self.root.geometry(SIZE_WINDOW)
        self.root.title("Gestor De Datos")
        self.root.configure(bg=DARK_COLOR)

        self.button_frame = tk.Frame(self.root, bg=DARK_COLOR)
        self.button_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=10, pady=10)

        self.display_frame = tk.Frame(self.root, bg=LIGHT_COLOR)
        self.display_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.crear_botones_principales()

    def crear_botones_principales(self):
        """
        Crea los botones principales de la aplicación para gestionar los datos.
        """
        for widget in self.button_frame.winfo_children():
            widget.destroy()

        tk.Label(self.button_frame, text="Gestion de Datos", bg=DARK_COLOR, fg=LIGHT_COLOR, font=FONT).pack(BUTTON_DIRECTION)

        tk.Button(
            self.button_frame,
            text="Visualizar registros",
            command=lambda: see(self.button_frame, self.display_frame, types, self.crear_botones_principales),
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT
        ).pack(BUTTON_DIRECTION)

        tk.Button(
            self.button_frame,
            text="Agregar registros",
            command=lambda: add(self.button_frame, self.display_frame, types, self.crear_botones_principales),
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT
        ).pack(BUTTON_DIRECTION)

        tk.Button(
            self.button_frame,
            text="Modificar registros",
            command=lambda: mod(self.button_frame, self.display_frame, types, self.crear_botones_principales),
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT
        ).pack(BUTTON_DIRECTION)

        tk.Button(
            self.button_frame,
            text="Eliminar registros",
            command=lambda: delete(self.button_frame, self.display_frame, types, self.crear_botones_principales),
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT
        ).pack(BUTTON_DIRECTION)

        tk.Button(
            self.button_frame,
            text="Salir",
            command=self.root.destroy,
            width=BUTTON_WIDTH,
            bg=LIGHT_COLOR,
            fg=DARK_COLOR,
            font=FONT
        ).pack(BUTTON_DIRECTION)
