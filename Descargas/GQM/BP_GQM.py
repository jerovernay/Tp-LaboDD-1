import duckdb
import pandas as pd


bibliotecas = pd.read_csv("bibliotecas_populares.csv")

con = duckdb.connect()

con.register("BP", bibliotecas)

bibliotecas.info()
print('\n Nulls en columnas: ')
print(bibliotecas.isnull().sum(), '\n')

""" Datos Irrelevantes """

# Columnas vacias

solo_nulls = con.query(
"""
SELECT COUNT(*) AS datos_existentes
FROM BP 
WHERE 
    piso IS NOT NULL OR
    informacion_adicional IS NOT NULL OR
    observacion IS NOT NULL OR
    subcategoria IS NOT NULL OR
    web IS NOT NULL;
""").to_df()

print("\nDatos no Null en las filas seleccionadas: ")
print(solo_nulls, '\n')

# Columnas con el mismo tipo de dato

mismo_dato_lat_long = con.query(
"""
SELECT DISTINCT tipo_latitud_longitud 
FROM BP
""").to_df()

mismo_dato_categoria = con.query(
"""
SELECT DISTINCT categoria 
FROM BP
""").to_df()

mismo_dato_anio_actualizacion = con.query(
"""
SELECT DISTINCT anio_actualizacion
FROM BP
""").to_df()

mismo_dato_fuente = con.query(
"""
SELECT DISTINCT fuente 
FROM BP
""").to_df()

print("Mismos datos siempre en: ")
print('\n',f"Latitud y longitud: {mismo_dato_lat_long}")
print('\n',f"categoria: {mismo_dato_categoria}")
print('\n',f"anio_actualizacion: {mismo_dato_anio_actualizacion}")
print('\n',f"fuente: {mismo_dato_fuente}", '\n')



""" Problema asociado al codigo postal """

# Calculamos el porcentaje de valores que son string y cuales son numericos

es_str = bibliotecas['cp'].apply(lambda x: isinstance(x, str)).mean() * 100
es_num = bibliotecas['cp'].apply(lambda x: isinstance(x, (int, float))).mean() * 100

print(f"Porcentaje de CP como string: {es_str:.2f}%")
print(f"Porcentaje de CP como numérico: {es_num:.2f}%", '\n')


cp_con_chars = bibliotecas['cp'].astype(str).str.contains(r'[^0-9]').mean() * 100

print(f"Porcentaje de CP con caracteres: {cp_con_chars:.2f}%", '\n')



""" Problema asociado a la consistencia interna de 3 columnas"""

# Localidades con el mismo nombre y distinta provincia

localidades_mismo_nombre = bibliotecas.groupby('localidad')['provincia'].nunique()  # agrupamos por localidad y provincia
problemas = localidades_mismo_nombre[localidades_mismo_nombre > 1].sort_values(ascending=False) # seleccionamos 

print(f"Localidades con mismo nombre en distintas provincias: {len(problemas)} ({len(problemas)/len(bibliotecas)*100:.2f}%)")
print("\nEjemplos de problemas (Localidad -> Cant. Provincias):")
print(problemas.head(7), '\n')


# Coincidencias de localidad con departamento y provincia

# Si la suma es igual a 2 entonces hay una unica combinacion de departamento y provincia para tal localidad.
# Si >2, hay al menos 2 combinaciones 
consistencia = bibliotecas.groupby('localidad')[['departamento', 'provincia']].nunique().sum(axis=1)
coincidencias = consistencia[consistencia > 2].sort_values(ascending= False)

print(
f"Localidades con inconsistencia geográfica: {len(coincidencias)} ({len(coincidencias)/len(bibliotecas)*100:.2f}%)"
, '\n') # 44
print("\n Ejemplos: ")
print(coincidencias.head(), '\n')

# Combinacion de provincia + localidad con multiples departamentos 

combinaciones_prov_loc = bibliotecas.groupby(['provincia', 'localidad'])['departamento'].nunique()
casos_problematicos = combinaciones_prov_loc[combinaciones_prov_loc > 1]

print(f"Combinaciones 'provincia' + 'localidad' con múltiples departamentos: {len(casos_problematicos)}")
print("\nEjemplos de casos problematicos: ")
print(casos_problematicos.head())















