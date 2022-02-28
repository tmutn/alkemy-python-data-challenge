# Alkemy Python Data Challenge #

Proyecto de alkemy.org para participar en la aceleración

URL del enunciado: https://drive.google.com/file/d/1ZxBnjsof8yCZx1JVLVaq5DbRjvIIvfJs/view

## Estructura del proyecto ##

El proyecto esta dividido en tres archivos, los tres pueden ser ejecutados individualmente:

* descargar_archivos.py
* init_sql.py
* normalizar_archivos.py

### descargar_archivos.py ###
* Obtiene los .csv de las URLs configuradas en el archivo .env

### init_sql.py ###
* Inicia la conexión a la base de datos y crea las tablas descritas en el archivo *create_tables.sql* 

### normalizar_archivos.py ###
* Utilizando descargar_archivos.py y init_sql.py, normaliza el contenido de acuerdo a los requerimientos del proyecto

Adicionalmente está el archivo **helpers.py** que tiene funciones auxiliares


## Instrucciones de instalación ##

1. Descargar el proyecto
2. Crear un entorno virtual con la consola usando [venv](https://docs.python.org/3/library/venv.html) en la ubicación del proyecto utilizando el comando:
    * ``` python3 -m venv /path/to/new/virtual/environment ```
3. Activar el entorno dependiendo de tu sistema operativo:
    * Activar el entorno virtual en Linux: ```$ source <venv>/bin/activate```
    * Activar el entorno virtual en Windows: ```C:\> <venv>\Scripts\activate.bat``` 
4. Una vez activado el entorno y dentro de la carpeta del proyecto, instalar las dependencias en el entorno utilizando:
    * ```pip install -r requirements.txt``` 
5. En el archivo .env ubicado en el directorio del proyecto, configurar usuario, contraseña y nombre de tu base de datos local postgresql
6. Ejecutar el archivo normalizar_archivos.py
    * ```python normalizar_archivos.py```
  
        
        



## Se asume... ##

En este apartado dejo una lista de asunciones que se tomaron a la hora de programar el desafío:

* Solo habrá archivos .csv de fuentes válidas dentro de los subdirectorios del proyecto para no interferir con la función de búsqueda
* Para los números de teléfono, solo importa lo que está dentro de la columa número_de_teléfono, dejando de lado el código de área
* El proyecto solo está configurado para aceptar las tres fuentes que indica el enunciado, debido a los nombres de las columnas que están hardcodeados
* Se deben corregir ciertos errores de los datos para su correcto procesamiento, por ejemplo nombres de provincias mal cargados, o la misma provincia con dos nombres distintos
* La base de datos es local y postgresql
