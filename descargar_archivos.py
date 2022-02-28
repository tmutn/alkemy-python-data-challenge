import datetime
from decouple import config, Csv
import requests
import re
import shutil
import os

#Módulos custom
import helpers

URL_LIST = config('URL_LIST', cast=Csv())

spanish_month_names = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre"
]

#Descarga archivos de la lista de URLs
def download_csv_files(URL_LIST=URL_LIST, month_names=spanish_month_names):
    actual_date = datetime.datetime.now()
    actual_day = actual_date.day
    actual_month = actual_date.month
    actual_year = actual_date.year
    for url in URL_LIST:
        r = requests.get(url, allow_redirects=True)
        file_name = re.findall("([^\/]+$)",url)[0] #Obtener lo que este después del último '/' del url
        category = file_name.split(".")[0] #Separar el nombre del archivo
        file_ext = file_name.split(".")[1] #Separar la extensión del archivo
        category_path = os.path.join(os.getcwd(), str(category), '')
        full_path = os.path.join(category_path, f"{str(actual_year)}-{month_names[actual_date.month-1]}", '')

        if "csv" in file_ext: #Descargar archivo y crear directorio si la extensión del archivo es CSV
            # Comprobar si ya existe un .csv dentro de la categoría
            if helpers.find_csv_in_directory(category, category_path):
                shutil.rmtree(category_path) #Eliminar la carpeta principal de la categoría para reemplazar

            # Crear directorios y descargar el archivos
            os.makedirs(os.path.dirname(full_path), exist_ok=True)             
            composite_filename = f'{category}-{actual_day}-{actual_month}-{actual_year}.{file_ext}'
            open(f'{os.path.join(full_path, composite_filename)}', 'wb').write(r.content)

download_csv_files()