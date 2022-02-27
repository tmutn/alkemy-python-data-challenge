import os
from decouple import config
from sqlalchemy import create_engine  
from sqlalchemy.sql import text

#Obtener variables de entorno
DB_TYPE = config('DB_TYPE', default = "postgresql")
DB_HOST = config('DB_HOST', default = "localhost")
DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')
DB_NAME = config('DB_NAME')

#Crear string de conexión
db_string = f'{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_string, encoding='utf-8')

#Ejecutar archivo DDL (si las tablas no existen)

try:
    with engine.connect() as con:
        file = open("create_tables.sql")
        query = text(file.read())
        con.execute(query)
        con.close
except Exception as err:
    print(err)