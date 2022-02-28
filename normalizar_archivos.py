from xml.etree.ElementInclude import include
import datetime as dt
from decouple import config
import pandas as pd
import os
import sqlalchemy.exc

#Módulos custom
import init_sql
import helpers
import descargar_archivos

#Obtener engine
engine = init_sql.get_engine()
#Generar tablas
init_sql.create_tables_from_script(engine)

#Descargar archivos
descargar_archivos.download_csv_files()
#Obtener el directorio root de este script
root_dir = os.path.dirname(os.path.abspath(__file__))
#Obtener la lista de categorias (buscando carpetas en el root_dir)
list_of_categories = helpers.find_folders_in_directory(root_dir)
#Obtener dict categoria : filepath para los csv encontrados en subdirectorios
category_filepath = helpers.get_category_filepath_dict(list_of_categories, root_dir)

#Excepción en caso de no haber archivos .csv
if category_filepath:
    pass
else:
    raise Exception("No hay archivos .csv descargados. Compruebe las urls en el archivo .env")

#Registros por fuente (para el punto 2)
dict_records_per_source = {}

#Cargar csv, eliminar columnas (opcional), asignar columnas nuevas (opcional)
def procesar_df(filepath, to_drop=None, df_cols=None):
    df = pd.read_csv(filepath[0], encoding='utf-8') #Leer fuente  
    if to_drop:
        df = helpers.drop_columns(df, to_drop)
    if df_cols:
        df.columns = df_cols
    return df

#Agregar fecha de actualizacion a un df
def add_datetime_col(df, colname='fecha_de_carga'):
    df[colname] = pd.Series([dt.datetime.now()] * len(df))
    return df


###############PUNTO 1###############
# Normalizar toda la información de Museos, Salas de Cine y Bibliotecas
# Populares, para crear una única tabla que contenga:
#   o cod_localidad
#   o id_provincia
#   o id_departamento
#   o categoría
#   o provincia
#   o localidad
#   o nombre
#   o domicilio
#   o código postal
#   o número de teléfono
#   o mail
#   o web
#####################################

#Columnas innecesarias para generar csv_normalizado
to_drop_punto1 = [
    'Observacion',
    'Observaciones',
    'Subcategoria',
    'subcategoria',
    'Departamento',
    'piso','Piso',
    'Cod_tel',
    'cod_area',
    'Información adicional',
    'Latitud',
    'Longitud',
    'TipoLatitudLongitud',
    'Info_adicional',
    'fuente',
    'Fuente',
    'jurisdiccion',
    'Tipo_gestion',
    'tipo_gestion',
    'año_inauguracion',
    'IDSInCA',
    'año_inicio',
    'Año_actualizacion',
    'Pantallas',
    'Butacas',
    'espacio_INCAA',
    'año_actualizacion'
]

#Columnas normalizadas para aplicar luego de sacar las innecesarias
db_cols_normalizado = [
    'cod_localidad',
    'id_provincia',
    'id_departamento',
    'categoria',
    'provincia',
    'localidad',
    'nombre',
    'domicilio',
    'codigo_postal',
    'numero_de_telefono',
    'mail',
    'web'
]

list_of_dfs = []
#Normalizar todos los cv y después unirlos en uno solo
for category, filepath in category_filepath.items():
    df_to_concat = procesar_df(filepath, to_drop_punto1, db_cols_normalizado)
    dict_records_per_source[category] = len(df_to_concat.index) #Para el punto 2
    list_of_dfs.append(df_to_concat)   
normalized_df = pd.concat(list_of_dfs, ignore_index=True)

#Agregar columna 'fecha_de_carga'
normalized_df = add_datetime_col(normalized_df)

#Volcar dataframe final del PUNTO 1 a la base de datos
normalized_df.to_sql(name='info_normalizada', con=engine, if_exists='replace', index=False)



###############PUNTO 2###############
# ● Procesar los datos conjuntos para poder generar una tabla con la siguiente
# información:
#   o Cantidad de registros totales por categoría
#   o Cantidad de registros totales por fuente
#   o Cantidad de registros por provincia y categoría
#####################################

#Se empieza desde el df normalizado en el primer punto
df_pto2 = normalized_df

#Columnas innecesarias
to_drop_punto2 = [
    "cod_localidad",
	"id_provincia" ,
	"id_departamento" ,
	"localidad",
	"nombre",
	"domicilio",
	"codigo_postal",
	"numero_de_telefono",
	"mail", 
    "web"
]

#Eliminar columnas innecesarias
df_pto2 = helpers.drop_columns(df_pto2, to_drop_punto2)

#Lista de tuplas con (nombre_incorrecto, nombre_correcto)
errores_provincias = [
    ('Santa Fe','Santa Fé'),
    ('Tierra del Fuego, Antártida e Islas del Atlántico Sur','Tierra del Fuego'),
    ('Neuquén\xa0','Neuquén'),
]

#Aplicar correcciones a los nombres de provincias
for error in errores_provincias:
    df_pto2['provincia'] = df_pto2['provincia'].str.replace(error[0],error[1])

#Obtener lista de categorías y de provincias para array de permutaciones
categorias = [item for item in df_pto2.categoria.unique()]
provincias = [item for item in df_pto2.provincia.unique()]

#Obtener las permutaciones de categorias y provincias
categorias_provincias = [[categoria,provincia] for categoria in categorias for provincia in provincias]

#Cantidad de categorias por provincia
provincia_categoria = []
for item in categorias_provincias:
    categoria = item[0]
    provincia = item[1]
    subset = df_pto2[(df_pto2['categoria']==categoria) & (df_pto2['provincia']==provincia)]
    provincia_categoria.append([categoria, provincia, f'{len(subset)}', None])

#Cantidad de registros totales por fuente
records_per_source = []
for key,value in dict_records_per_source.items():
    records_per_source.append([None,None,value,key])

#Cantidad de registros por categoria
records_per_category = []
for categoria in categorias:
    subset = df_pto2[df_pto2['categoria']==categoria]
    records_per_category.append([categoria, None, f'{len(subset)}', 'Fuente Normalizada'])

#Columnas normalizadas del punto 2
columns = [
    'categoria', 
    'provincia',
    'cantidad_total',
    'fuente'
]

#Crear dataframes, agregarlos a una lista y concatenar
df_records_per_source = pd.DataFrame(columns=columns, data=records_per_source)
df_records_per_category = pd.DataFrame(columns=columns, data=records_per_category)
df_provincia_categoria = pd.DataFrame(columns=columns, data=provincia_categoria)
lista_dfs = [df_records_per_source, df_records_per_category, df_provincia_categoria]
df_cantidades = pd.concat(lista_dfs, ignore_index=True)

#Agregar columna 'fecha_de_carga'
df_cantidades = add_datetime_col(df_cantidades)

#Volcar dataframe final del PUNTO 2 a la base de datos
df_cantidades.to_sql(name='info_registros', con=engine, if_exists='replace', index=False)

###############PUNTO 3###############
# ● Procesar la información de cines para poder crear una tabla que contenga:
#   o Provincia
#   o Cantidad de pantallas
#   o Cantidad de butacas
#   o Cantidad de espacios INCAA
#####################################

#Columnas innecesarias
to_drop_punto3 = [
    'Cod_Loc',
    'IdProvincia',
    'IdDepartamento',
    'Observaciones',
    'Categoría',
    'Departamento',
    'Dirección',
    'Localidad',
    'Nombre',
    'Domicilio',
    'Piso',
    'CP',
    'cod_area',
    'Teléfono',
    'Mail',
    'Web',
    'Información adicional',
    'Latitud',
    'Longitud',
    'TipoLatitudLongitud',
    'Fuente',
    'tipo_gestion',
    'año_actualizacion' 
]

#Columnas normalizadas del punto 3
db_cols_cine = [
	'provincia',
	'cant_pantallas',
	'cant_butacas',
	'espacios_incaa'
]

#Buscar csv de cines
dir_csv_cine = helpers.find_csv_in_directory('cine', root_dir)

#Procesar df de cines
df_cines = procesar_df(dir_csv_cine, to_drop_punto3, db_cols_cine)

#Agregar columna fecha_de_carga
df_cines = add_datetime_col(df_cines)

#Volcar dataframe final del PUNTO 3 a la base de datos
df_cines.to_sql(name='info_cines', con=engine, if_exists='replace', index=False)
