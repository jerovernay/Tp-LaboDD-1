""" Metricas del Analisis de Calidad de Datos para el GQM """

import duckdb
import pandas as pd
import os

# Antes de continuar les pedimos que eb la siguiente linea completen con el path donde se encuentre guardada la carpeta Tp-LaboDD-1
# para poder correr el codigo sin problemas.
os.chdir(r"C:\Users\Dell\Escritorio")       

#Leemos los Csv's originales
bibliotecas = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\bibliotecas_populares.csv")
establecimientos_ed = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\2025.04.08_padron_oficial_establecimientos_educativos_die.csv")


# Conectamos a Duckdb y registramos 
con = duckdb.connect()
con.register("EE", establecimientos_ed)
con.register("BP", bibliotecas)



"""===== Metricas para el Analisis de Calidad de Datos de Establecimientos Educativos ====="""

print("Veamos los datos en crudo: ")
establecimientos_ed.info()
print('\n Nulls en columnas: ')
print(establecimientos_ed.isnull().sum(), '\n')



""" Problemas asociado con los Correos """

Registros_totales = len(establecimientos_ed)
mails_nulls = establecimientos_ed["Mail"].isnull().sum()
completitud_mails = mails_nulls / Registros_totales * 100

print("\nPorcentaje de establecimientos sin correo electronico: ")
print(round(completitud_mails,2), '\n')


mails_mal_formato_total = """
/*Cuento los correos validos, los multiplico para convertirlo en porcentaje y 
 divido sobre el total de mails no nulos*/
SELECT 
    COUNT(*) * 100.0 / (
    SELECT COUNT(*) FROM EE WHERE "Mail" IS NOT NULL
    ) AS porcentaje_mail_totalmente_invalidos
FROM EE
/*Aca debajo, cuento los mails validos*/
WHERE "Mail" IS NOT NULL
  AND INSTR("Mail", '@') = 0;
"""


cant_mails_mal_totalmente = con.query(mails_mal_formato_total).to_df()
print(cant_mails_mal_totalmente, '\n') 

""" Calculamos el porcentaje de mails validos """

# Teniendo en cuenta que los mails son 81.71 validos, 
# le multiplicamos el porcentaje que tiene al menos un arroba 

porcentaje_EE_mail_valido = (99.65 * 81.71 / 100)
print(f"Porcentaje de mail validos: {porcentaje_EE_mail_valido: .2f}%", '\n') 




""" Problemas asociado a el domicilio """

# Porcentaje de domicilios sin numero 

sin_numero = establecimientos_ed["Domicilio"].apply(lambda x: not any(char.isdigit() for char in str(x)))
porcentaje_sin_numero = sin_numero.mean() * 100

print(f"\nPorcentaje de Domicilios sin Numero: {porcentaje_sin_numero: .2f}%", '\n') 


# Porcentaje de domicilios con sintexis invalida("falsos")

chars_invalidos = {'?', '%', '$', '#', '@', '*', ';','^', '!', '<', '>'}
patron_invalido = {'', ' '}

def es_invalido(x):
    
    x_str = str(x).strip().lower()      #Normalizacion del String
    
    return(x_str in patron_invalido or  any(y in str(x) for y in chars_invalidos)) # Vemos si esta vacio o tiene cosas que no deben estar


invalidos = establecimientos_ed["Domicilio"].apply(es_invalido)
porcentaje_invalido = invalidos.mean() * 100

print(f"\nPorcentaje de Domicilios Invalidos: {porcentaje_invalido: .2f}%", '\n')






"""===== Metricas para el Analisis de Calidad de Datos de Bibliotecas Populares ====="""



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