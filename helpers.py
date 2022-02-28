import os
import pandas as pd

#Obtener la lista de carpetas en un directorio, sin recursión
def find_folders_in_directory(target_directory):
    dir_contents = os.listdir(target_directory)
    directories = [item for item in dir_contents if os.path.isdir(item)]
    return directories

#Buscar todos los .csv dentro de la lista de carpetas
def find_csv_in_directory(categoria, directory):
    csv_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv") and categoria in file:
                csv_list.append(os.path.join(root, file))
    return csv_list

#Armar un diccionario con el nombre de la categoria como key y el path al csv como value
def get_category_filepath_dict(list_of_categories, directory):
    category_filepath = {}
    for category in list_of_categories:
        file_path = find_csv_in_directory(category, directory)
        if file_path: #Solo agregar directorios que tengan un .csv
            category_filepath[f"{category}"] = file_path
    return category_filepath

def drop_columns(df, to_drop):
    for column in to_drop:
        try:
            df.drop(column, inplace=True, axis=1)
        except KeyError as key_error:
            pass
            # print(f"{key_error}, skipping column drop")
    return df