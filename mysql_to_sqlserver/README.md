# mysql_to_sqlserver

 incluye un script en Python que permite migrar datos de una base de datos MySQL a una nueva base de datos en SQL Server. Dentro de la carpeta `mysql_to_sqlserver` están cuatro archivos; en el archivo llamado `config.py` se modifican las credenciales de MySQL y SQL Server para realizar la migración. En `mssql_connection.py` se conecta a SQL Server y en `mysql_connection.py` se conecta a MySQL. Finalmente, en `value_migrate.py` ocurre la migración, estableciendo una conexión con la base de datos al iniciar y cerrándola al salir, manejando errores.

Se han implementado pruebas para asegurar que todas las funcionalidades de la interfaz funcionen correctamente:

## Defensa de Dariana Pol Aramayo

Agregar una columna "parentesis" a la tabla "genre" donde deben estar los valores de la columna "name" con el formato de parentesis intermedios.
"graficos/image.png"

## Gráficos de mysql_to_sqlserver

### Diagrama de Clases - Conexión MySQL
![image](https://github.com/user-attachments/assets/b962ca43-de6c-4f03-bf89-0d327862c2ab)


### Diagrama de Clases - Conexión SQL Server
![image](https://github.com/user-attachments/assets/3dedfc03-8db7-477b-bc0b-9d5c38734cd7)

    

### Pseudocodigos - Clase: mysql_connection 
    // Método para conectar a la base de datos MySQL
    FUNCIÓN connect()
        INTENTAR
            Establecer conexión a la base de datos MySQL usando las configuraciones
        EXCEPCIÓN Error e
            Imprimir "Error: " + mensaje de error
        FIN INTENTAR
    FIN FUNCIÓN
### Pseudocodigos - Clase: mssql_connection
// Método para conectar a la base de datos MS SQL Server
FUNCIÓN connect()
    INTENTAR
        Conectar a la base de datos 
        Obtener el nombre de la base de datos de la configuración de MySQL
        Crear una consulta SQL para crear la base de datos si no existe en SQL server
        Usar una conexión separada para crear la base de datos
            Crear un cursor para la conexión
            Ejecutar la consulta SQL para crear la base de datos

        Reconectar a la base de datos recién creada o existente
    EXCEPCIÓN Error e
        Imprimir "Error: " + mensaje de error
    FIN INTENTAR
FIN FUNCIÓN
### Pseudocodigos usados en - Clase: mysql_connection y Clase: mssql_connection
    // Método para cerrar la conexión 
    FUNCIÓN close()
        SI la conexión existe
            Cerrar la conexión a la base de datos MySQL
        FIN SI
    FIN FUNCIÓN


    // Método para ejecutar una consulta SQL y devolver los resultados
    FUNCIÓN execute_query(query)
        INTENTAR
            Crear un cursor 
            Ejecutar la consulta SQL usando el cursor
            Obtener todos los resultados de la consulta
            Devolver los resultados
        EXCEPCIÓN Error e
            Imprimir "Error: " + mensaje de error
            Devolver NULO
        FIN INTENTAR
    FIN FUNCIÓN


    // Método para ejecutar una consulta SQL de actualización (INSERT, UPDATE, DELETE)
    FUNCIÓN execute_update(query)
        INTENTAR
            Crear un cursor para la conexión
            Ejecutar la consulta SQL de actualización usando el cursor
            Confirmar la transacción en la base de datos
        EXCEPCIÓN Error e
            Imprimir "Error: " + mensaje de error
        FIN INTENTAR
    FIN FUNCIÓN

### Pseudocodigos - mssql_connection
    // Método para cerrar la conexión a la base de datos MS SQL Server
    FUNCIÓN close
        SI self.connection NO ES NULO ENTONCES
            self.connection.cerrar()
            IMPRIMIR 'Conexión a MS SQL Server cerrada'
    FIN FUNCIÓN

    // Método para ejecutar una consulta SQL y devolver los resultados
    FUNCIÓN execute_query(consulta)
        TRATAR
            cursor = self.connection.cursor()
            cursor.ejecutar(consulta)
            resultado = cursor.obtener_todos()
            RETORNAR resultado
        EXCEPTO ERROR COMO e
            IMPRIMIR 'Error: ' + e
            RETORNAR NULO
    FIN FUNCIÓN

    // Método para ejecutar una consulta SQL de actualización (INSERT, UPDATE, DELETE)
    FUNCIÓN execute_update(consulta)
        TRATAR
            cursor = self.connection.cursor()
            cursor.ejecutar(consulta)
            self.connection.confirmar()
        EXCEPTO ERROR COMO e
            IMPRIMIR 'Error: ' + e
    FIN FUNCIÓN

### Pseudocodigos - recursion
FUNCIÓN invert_string(s)
    SI la longitud de s es menor o igual a 1
        Devolver s
    SINO
        Devolver invert_string(s sin el primer carácter) + el primer carácter de s
    FIN CONDICION
FIN FUNCIÓN

### Pseudocodigo - test_migrate
FUNCIÓN test_migrate_data()
    Crear conexión con MySQL y MS SQL Server
    INTENTAR
        Conectar con MySQL y MS SQL Server
        Obtener el nombre de la base de datos de la configuración de MySQL
        Usar la base de datos en MS SQL Server
        Obtener nombres de tablas de MySQL y MS SQL Server
        Inicializar contadores de registros totales para MySQL y MS SQL Server
        PARA cada tabla en MySQL
            Obtener el nombre de la tabla
            Obtener todos los datos de la tabla en MySQL
            Sumar el número de registros de MySQL al contador total

            Obtener todos los datos de la tabla en MS SQL Server 
            Sumar el número de registros de MS SQL Server al contador total

            Asegurarse de que el número de registros en MySQL y MS SQL Server coincida en cada tabla
        FIN 

        Asegurarse de que el número total de registros en MySQL y MS SQL Server coincida
        Imprimir el número total de registros en MySQL y MS SQL Server
        Imprimir "Todas las pruebas pasaron exitosamente."
    EXCEPCIÓN Error e
        Imprimir "Error durante la prueba: " + mensaje de error
    FIN INTENTAR
    Cerrar la conexión a MySQL y MS SQL Server
FIN FUNCIÓN

### Pseudocodigo - value_migrate
FUNCIÓN migrate_data()
    Crear conexión a MySQL y MS SQL Server
    INTENTAR
        Conectar a MySQL y  MS SQL Server
        Obtener el nombre de la base de datos de la configuración de MySQL
        Usar la base de datos en MS SQL Server

        Verificar si la base de datos MS SQL Server está vacía
        Obtener nombres de tablas de MySQL
        Obtener nombres de tablas de MS SQL Server

        SI la base de datos MS SQL Server está vacía
            Obtener los nombres de las tablas, llaves primarias y foraneas de la base de datos de MySQL
            y guardar la informacion en variables

            INTENTAR
                PARA cada tabla en MySQL
                    Obtener el nombre de la tabla
                    Verificar si la tabla existe en MS SQL Server
                    SI la tabla no existe
                        Obtener esquema de tabla de MySQL
                        Obtener información de clave primaria
                        Obtener información de clave foránea

                        SI la tabla tiene claves foráneas
                            Agregar claves foráneas a la lista

                        Crear tabla en MS SQL Server con tipos de datos corregidos
                        Agregar restricción de clave primaria 

                EXCEPCIÓN Error e
                    Revertir transacción en caso de error
                    Imprimir "Error durante la creación de la tabla o la adición de claves foráneas: " + mensaje de error
                    RETORNAR

            Insertar datos en las tablas
            PARA cada tabla en MySQL
                Obtener el nombre de la tabla
                Obtener columnas de la tabla
                PARA cada fila en los columnas
                    Crear consulta de inserción
                    INTENTAR
                        Ejecutar consulta de inserción en MS SQL Server
                    EXCEPCIÓN Error e
                        Imprimir "Error: " + mensaje de error

            Agregar restricciones de clave foránea a MS SQL Server
            PARA cada clave foránea en la lista
                Crear consulta de alteración de tabla para agregar clave foránea
                INTENTAR
                    Ejecutar consulta de alteración de tabla en MS SQL Server
                    Imprimir "Restricción de clave foránea agregada exitosamente a la tabla " + nombre de la tabla
                EXCEPCIÓN Error e
                    Imprimir "Error al agregar restricción de clave foránea a la tabla " + nombre de la tabla + ": " + mensaje de error

            Ejecutar pruebas de migración de datos

--DEFENSA DEL PROYECTO 
            Agregar columna "parentesis" a la tabla "genre"
            Crear consulta de alteración de tabla para agregar columna "parentesis"
            INTENTAR
                Ejecutar consulta de alteración de tabla en MS SQL Server\
            EXCEPCIÓN Error e
                Imprimir "Error al agregar columna 'parentesis' a la tabla genre: " + mensaje de error

            Actualizar columna "parentesis" con valores invertidos
            Crear consulta para seleccionar valores de la columna "name"
            Obtener registros de la consulta
            PARA cada registro
                Obtener valor de la columna "name"
                Invertir el valor de la columna "name"
                Crear consulta de actualización para la columna "parentesis"
                INTENTAR
                    Ejecutar consulta de actualización en MS SQL Server
                    Imprimir "Registro hecho exitosamente en la tabla genre."
                EXCEPCIÓN Error e
                    Imprimir "Error al actualizar valores en la columna 'parentesis': " + mensaje de error

--FIN --DEFENSA DEL PROYECTO 

            Agregar columna "modificación" a cada tabla existente
            PARA cada tabla en MySQL
                Obtener el nombre de la tabla
                Imprimir el nombre de la tabla a la que se está agregando la columna "modificación"

                Verificar si la columna "modificación" ya existe
                SI la columna no existe
                    Crear consulta de alteración de tabla para agregar columna "modificación"
                    INTENTAR
                        Ejecutar consulta de alteración de tabla en MS SQL Server
                        Imprimir "Columna 'modificación' agregada a la tabla: " + nombre de la tabla
                    EXCEPCIÓN Error e
                        Imprimir "Error al agregar columna 'modificación' a la tabla " + nombre de la tabla + ": " + mensaje de error

                Obtener la fecha actual en el formato DD-MM-YYYY
                Crear consulta de actualización para la columna "modificación"
                INTENTAR
                    Ejecutar consulta de actualización en MS SQL Server
                    Imprimir "Columna 'modificación' actualizada con la fecha actual en la tabla: " + nombre de la tabla
                EXCEPCIÓN Error e
                    Imprimir "Error al actualizar la columna 'modificación' en la tabla " + nombre de la tabla + ": " + mensaje de error

        EXCEPCIÓN Error e
            Imprimir "Error durante la migración: " + mensaje de error
    FIN INTENTAR

    Cerrar la conexión a MySQL
    Cerrar la conexión a MS SQL Server
FIN FUNCIÓN


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

