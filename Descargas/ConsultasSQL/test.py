import duckdb
import pandas as pd
import os

os.chdir(r"C:\Users\Dell\Escritorio\Tp-LaboDD-1\Descargas") 
# Aca pongan el directorio que esten usando, asi con cambiar esto solo ya nos core en cualquier compu 
print("Current working directory:", os.getcwd(), '\n')
# print("Files in current directory:", os.listdir(), '\n')


#Leo el csv de Bibliotecas Populares
bibliotecas = pd.read_csv(r"TablasOriginales\bibliotecas_populares.csv", dtype={'id_provincia': str, 'id_departamento': str})
establecimientos_ed = pd.read_csv(r"TablasOriginales\2025.04.08_padron_oficial_establecimientos_educativos_die.csv", dtype={'id_departamento': str})
padron = pd.read_csv(r"TablasOriginales\padron_poblacion.csv", dtype={'id_departamento': str})
provincias = pd.read_csv(r"TablasModificadas\Provincia.csv",dtype={'id':str})

con = duckdb.connect()
con.register("Bibliotecas", bibliotecas)




# Aca les dejo si quieren ver mas a fondo las columnas de los csv con los tipos de datos, nulls y demas 
# Hay una banda de datos al pedo

"""
print("Info sobre Bp:")
print(bibliotecas.info())  

print("Info sobre Ee:")
print(establecimientos_ed.info())

print("Info sobre padron:")
print(padron.info())

"""

consultaSQL = """
               SELECT DISTINCT id_departamento,departamento
               FROM padron
               """

dataframeResultado = duckdb.query(consultaSQL).df()

# Ejecutar la consulta en DuckDB
resultado = duckdb.query("""
    SELECT DISTINCT
        id_provincia, 
    FROM bibliotecas
""").to_df()

# Exportar a CSV
resultado.to_csv("departamentos_con_provincia.csv", index=False)




