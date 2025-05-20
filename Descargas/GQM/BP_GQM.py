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
print('\n',f"fuente: {mismo_dato_fuente}")



