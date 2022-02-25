from decouple import config
import helpers
import pandas as pd
import os
from sqlalchemy import create_engine  

#Obtener variables de entorno
DB_TYPE = config('DB_TYPE', default = "postgresql")
DB_HOST = config('DB_HOST', default = "localhost")
DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')
DB_NAME = config('DB_NAME')
#Crear string de conexión
db_string = f'{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_string)

#Lista de las columnas innecesarias de los CSV para sacarlas de los df
to_drop_punto1 = ['Observacion',
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
            'año_actualizacion']

#Lista de columnas que se aplicarán a los archivos para normalizar y poder volcar a la base de datos
db_cols_normalizado = ['cod_localidad',
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
            'web']

#Obtener el directorio root de este script
root_dir = os.path.dirname(os.path.abspath(__file__))

#Obtener la lista de categorias (buscando carpetas en el root_dir)
list_of_categories = helpers.find_folders_in_directory(root_dir)

#Obtener dict
category_filepath = helpers.get_category_filepath_dict(list_of_categories, root_dir)

#Importante para el punto 2
dict_records_per_source = {}

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

#Generar csv_normalizado
def procesar_df(filepath, to_drop=None, df_cols=None):
    df = pd.read_csv(filepath[0], encoding='utf-8') #Leer fuente  
    #Eliminar columnas innecesarias de la fuente
    if to_drop:
        df = helpers.drop_columns(df, to_drop)
    #Normalizar las columnas con los nombres utilizados en la base de datos
    if df_cols:
        df.columns = df_cols
    return df

#Normalizar todos los cv y después unirlos en uno solo
list_of_dfs = []
for category, filepath in category_filepath.items():
    df_to_concat = procesar_df(filepath, to_drop_punto1, db_cols_normalizado)
    dict_records_per_source[category] = len(df_to_concat.index) #Para el punto 2
    list_of_dfs.append(df_to_concat)   
normalized_df = pd.concat(list_of_dfs, ignore_index=True)

#Volcar contenido normalizado unificado a la base de datos
normalized_df.to_sql(name='info_normalizada', con=engine, if_exists='replace', index=False)



###############PUNTO 2###############
# ● Procesar los datos conjuntos para poder generar una tabla con la siguiente
# información:
#   o Cantidad de registros totales por categoría
#   o Cantidad de registros totales por fuente
#   o Cantidad de registros por provincia y categoría
#####################################

df = pd.read_csv("normalized_csv.csv", encoding='utf-8')
to_drop_punto2=["cod_localidad",
	"id_provincia" ,
	"id_departamento" ,
	"localidad",
	"nombre",
	"domicilio",
	"codigo_postal",
	"numero_de_telefono",
	"mail", 
    "web"]

df = helpers.drop_columns(df, to_drop_punto2)

errores_provincias = [
        ['Santa Fe','Santa Fé'],
        ['Tierra del Fuego, Antártida e Islas del Atlántico Sur','Tierra del Fuego'],
        ['Neuquén\xa0','Neuquén'],
]

for error in errores_provincias:
    df['provincia'] = df['provincia'].str.replace(error[0],error[1])

categorias = [item for item in df.categoria.unique()]
provincias = [item for item in df.provincia.unique()]

#Hacer todas las permutaciones de categorias y provincias
categorias_provincias = [[categoria,provincia] for categoria in categorias for provincia in provincias]

#Cantidad de categorias por provincia
provincia_categoria = []
for item in categorias_provincias:
    categoria = item[0]
    provincia = item[1]
    subset = df[(df['categoria']==categoria) & (df['provincia']==provincia)]
    provincia_categoria.append([categoria, provincia, f'{len(subset)}', None])

#Cantidad de registros totales por fuente
records_per_source = []
for key,value in dict_records_per_source.items():
    records_per_source.append([None,None,value,key])

#Cantidad de registros por categoria
records_per_category = []
for categoria in categorias:
    subset = df[df['categoria']==categoria]
    records_per_category.append([categoria, None, f'{len(subset)}', 'Fuente Normalizada'])

#Columnas para el df final del punto 2
columns = ['categoria',  'provincia', 'cantidad_total', 'fuente',]

df_records_per_source = pd.DataFrame(columns=columns, data=records_per_source)
df_records_per_category = pd.DataFrame(columns=columns, data=records_per_category)
df_provincia_categoria = pd.DataFrame(columns=columns, data=provincia_categoria)

lista_dfs = [df_records_per_source, df_records_per_category, df_provincia_categoria]

df_cantidades = pd.concat(lista_dfs, ignore_index=True)

df_cantidades.to_sql(name='info_registros', con=engine, if_exists='replace', index=False)

###############PUNTO 3###############
# ● Procesar la información de cines para poder crear una tabla que contenga:
#   o Provincia
#   o Cantidad de pantallas
#   o Cantidad de butacas
#   o Cantidad de espacios INCAA
#####################################

to_drop_punto3 = [
    'Cod_Loc',
    'IdProvincia',
    'IdDepartamento',
    'Observaciones',
    'Categoría',
    'Departamento',
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

db_cols_cine = [
	'provincia',
	'cant_pantallas',
	'cant_butacas',
	'cant_espacios_incaa'
]

dir_csv_cine = helpers.find_csv_in_directory('cine', root_dir)
df_cines = procesar_df(dir_csv_cine, to_drop_punto3, db_cols_cine)
df_cines.to_sql

df_cines.to_sql(name='info_cines', con=engine, if_exists='replace', index=False)