# thinker_app
Incluye un script en Python que permite crear una interfaz para las interraciones y modificaiones de una base de datos. Dentro de la carpeta tkinter_app están ocho archivos; en add.py se agrega una nueva fila a la tabla seleccionada, en conect.py se encuentra la conexion a la base de datos, const.py se encuentran llos valores constantes que se usan a lo largo de todo el codigo, delete.py nos permite eliminar una fila de la tabla seleccionada, bottons.py nos permite la creacion de la ventana principal y los botones principales, mod.py nos permite modificar los datos de la tabla seleccionada, see.py nos permite visualizar los registros de las tablas y por ultimo main.py llama a la interfaz

## Diagrama de Clases
![image](https://github.com/user-attachments/assets/38d790e9-dacc-4600-bc90-f35c60b5586a)

## Pseudocodigo
    Si se llama a "main" entonces
	    Se crea la ventana principal llamada "root" y se llama a App(root)

    FUNCION App(root):
	    Se declaran las variables necesarias para la interfaz y se llama a la función crear_botones_principales

    FUNCION crear_botones_principales(self):
	    Se crea la etiqueta llamada "Gestion de Datos"
	    Se crea el botón llamado "Visualiza registros" que cuando se lo presiona se llama a la función "see"
	      FUNCION see:
		      Se crea un boton para cada tabla de la base que al presionarlo se llama a la función "display_records"
			      FUNCION display_records:
				      Se conecta a la base de datos
				      Se selecciona todo de la tabla seleccionada y se lo muestra en el recuadro de la izquierda de la pestaña principal
				      Se deconecta a la base de datos
 		          Se crea un botón llamado "Volver" donde cada vez que se lo presiona se llama a la función "crear_botones_principales"
        FIN FUNCION
	    Se crea el botón "Agregar registros" que cuando se lo presiona se llama a la funcion "add:
	      FUNCION add:
		      Se crea un botón para cada tabla de la base que al presionarlos se llama a la función "add_record"
		        FUNCION add_record:
				      Se crea una ventana secundaria 
				      Se conecta a la base de datos
				      Se selecciona todos los registros menos la ultima columna  
				        En la nueva ventana se crean entradas de datos para que el usuario pueda meter los datos que quiere agregar, un botón para agregar los datos y un botón para volver   
					      Si el id esta repetido se manda un mensaje de error
    				  En la ultima columna "modificación"
					      Cada vez que se agregue una nueva fila se guarda la fecha de modificacion
 				    Se cierra la conexión con la base de datos y se cierra la ventana secundaria
         FIN FUNCION
	    Se crea un botón llamado "Modificar registros" cada vez que se lo presiona se llama a la función "modificate"   
		    FUNCION mod:
			    Se crea un botón para cada tabla de la base que al presionarlos se llama a la función "mod_record"
				    FUNCION mod_record:
					    Se crea un ventana secundaria
					    Se conecta a la base de datos
					    Se pide al usuario ingresar la llave primaria de la tabla seleccionada y se crean entradas de los datos que puede modificar
						    Si no existe la llave primaria se muestra un mensaje de error
					    Se modifica el registro con los nuevos datos y se muestra la nueva tabla con las modificaciones
			      Se cierra la conexión con la base de datos y se cierra la ventana secundaria
         FIN FUNCION
	  Se crea un botón llamado "Eliminar registros" que al presionarlo se llama a la función "delete"
		  FUNCION delete:
			  Se crea un botón por cada tabla de la base que al presionarlos se llama a la función "delete_record"
				  FUNCION delete_record:
					  Se crea una ventana secundaria
					  Se conecta a la base de datos
					  Se pide al usuario que ingrese la llave primaria de la fila que quiera eliminar y se crea un botón llamado "eliminar"
						  Si se presiona e botón se selecciona toda la fila y se la elimina
					  Se destruye la ventana secundaria
					  Se muestra la tabla actualizada
			    Se crea un botón de "volver" que cuando se lo presiona se llama a la función crear_botones_principales
         FIN FUNCION
	  Se crea un botón llamado "Salir" que cuando se lo presiona se cierra toda la interfaz

      root.mainloop()
## Colaboradoras

- **Nicole Lozada León**  
  Email: [nicole.lozada@ucb.edu.bo](mailto:nicole.lozada@ucb.edu.bo)

- **Dariana Pol Aramayo**  
  Email: [dariana.pol@ucb.edu.bo](mailto:dariana.pol@ucb.edu.bo)

## Universidad

**Universidad Católica Boliviana**  
Ciudad: Santa Cruz

## Conclusión

Este proyecto ha logrado modernizar y optimizar la gestión de datos en la empresa, asegurando que el nuevo sistema sea robusto, eficiente y fácil de usar. La interfaz gráfica y la migración de datos han sido implementadas con éxito, proporcionando una solución confiable para la gestión de información.
