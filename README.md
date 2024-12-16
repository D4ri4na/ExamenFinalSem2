# Modernización del Sistema de Gestión de Datos

## Descripción del Proyecto

Este proyecto tiene como objetivo modernizar un sistema heredado que gestiona datos, mejorando su eficiencia, confiabilidad y usabilidad. Se ha diseñado e implementado un nuevo sistema utilizando una base de datos relacional y una aplicación en Python, garantizando una interfaz fácil de usar para la gestión de datos.

## Funcionalidades del Proyecto

### 1. Migración de Datos

El sistema incluye un script en Python que permite migrar datos de una base de datos MySQL a una nueva base de datos en SQL Server. Dentro de la carpeta `mysql_to_sqlserver` están cuatro archivos; en el archivo llamado `config.py` se modifican las credenciales de MySQL y SQL Server para realizar la migración. En `mssql_connection.py` se conecta a SQL Server y en `mysql_connection.py` se conecta a MySQL. Finalmente, en `value_migrate.py` ocurre la migración, estableciendo una conexión con la base de datos al iniciar y cerrándola al salir, manejando errores.

### 2. Interfaz de Usuario con Python Tkinter

Se ha desarrollado una interfaz gráfica utilizando la biblioteca Tkinter de Python, permitiendo a los usuarios gestionar la base de datos migrada con las siguientes funcionalidades:

- **Visualización de Registros**: Muestra los datos de las tablas en una lista o tabla dentro de la interfaz.
- **Adición de Registros**: Proporciona un formulario para ingresar nuevos datos en las tablas.
- **Modificación de Registros**: Permite la edición de registros existentes de forma intuitiva.
- **Eliminación de Registros**: Facilita la eliminación segura de registros seleccionados.

## Pruebas de la Solución

Se han implementado pruebas para asegurar que todas las funcionalidades de la interfaz funcionen correctamente:

1. **Pruebas de las Acciones Principales**:
   - Verificación de que los datos se muestren correctamente en la interfaz.
     ![image](https://github.com/user-attachments/assets/db1119a3-2800-4137-89c9-e88df68eca37)
   - Confirmación de que las operaciones de adición, modificación y eliminación actualicen la base de datos.
     
     **Agregar**
     ![image](https://github.com/user-attachments/assets/5b35fa48-4587-4924-b32d-63eb39161929)
     ![image](https://github.com/user-attachments/assets/208d03f9-0851-43ab-9280-cce473b795e1)

     
     **Modificar**
     ![image](https://github.com/user-attachments/assets/5031cc03-a6d9-4970-b9ca-da44747c17b4)
     ![image](https://github.com/user-attachments/assets/9bf8ebd5-8e94-4e0f-9807-e3b3272e5c91)
     **Eliminar**
     ![image](https://github.com/user-attachments/assets/088da59c-db4a-47bd-8a6a-6c2e0ccb111e)
     ![image](https://github.com/user-attachments/assets/6f4e6a33-550b-40c0-bae3-930e5eeb904f)

2. **Validación de Entradas**:
   - Manejo adecuado de errores de entrada (campos vacíos, formatos incorrectos, etc.).
     ![image](https://github.com/user-attachments/assets/249b5bff-a72b-4518-a324-327ce337c881)
   - Mensajes claros en caso de errores.
     ![image](https://github.com/user-attachments/assets/cb9b2672-c249-4e4c-a0c4-7e1934a1780e)


3. **Pruebas de Integridad de Datos**:
   - Verificación de que los cambios realizados desde la interfaz se reflejen correctamente en la base de datos.
     ![image](https://github.com/user-attachments/assets/ff7e6c00-6997-4b3d-b2ef-2e8e7400a5b7)

4. **Pruebas de Cierre de Conexión**:
   - Verificación de que la conexión a la base de datos se cierre correctamente al cerrar la aplicación.
     ![image](https://github.com/user-attachments/assets/67977ba6-4d44-45ad-b3e8-00073162761d)

## Gráficos del Proyecto

A continuación se presentan los diagramas que ilustran la estructura y el funcionamiento del sistema:

### Modelo de Entidad-Relación
<img src="graficos/modelo_E-T.jpeg" alt="Modelo de Entidad-Relación" width="300" style="display: block; margin: auto;">

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

