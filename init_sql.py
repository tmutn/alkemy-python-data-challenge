import os
from decouple import config
import sqlalchemy.exc
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

def create_tables_from_script(engine):
    try:
        with engine.connect() as con:
            file = open("create_tables.sql")
            query = text(file.read())
            con.execute(query)
            con.close
    except sqlalchemy.exc.OperationalError as err:
        if f'database "{DB_NAME}" does not exist' in str(err.__str__):
            raise Exception(f'Error: La base de datos {DB_NAME} no existe\n')
        if 'authentication failed' in str(err.__str__):
            raise Exception('Error de autenticación: Comprueba el que el usuario y la contraseña de la base de datos sean correctos\n')
        else:
            raise Exception(err)

def get_engine():
    return engine

create_tables_from_script(engine)