Se asume que...

Solo habrá archivos .csv de fuentes válidas dentro de los subdirectorios del proyecto para no interferir con la función de búsqueda.



El código de área no importa para el número de teléfono
El teléfono debe ser de tipo VARCHAR en la base de datos ya que muchos teléfonos incluyen el término INT para denotar el interno
Las únicas carpetas en la estructura del código son las que albergan en algún lugar de su estructura un archivo csv

El archivo csv tiene errores de sintáxis/uniformidad en la carga de las provicias y deben ser corregidos:
['Santa_Fe','Santa_Fé']
['Tierra_del_Fuego','Tierra_del_Fuego_Antártida_e_Islas_del_Atlántico_Sur']
['Neuquén', 'Neuquén\xa0']
