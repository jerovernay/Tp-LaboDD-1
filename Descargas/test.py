import duckdb
import pandas as pd
import os


""" Lectura de Archivos """

#os.chdir(r"C:\Users\Dell\Escritorio\Tp-LaboDD-1\Descargas") # Aca pongan el directorio que esten usando, asi con cambiar esto solo ya nos core en cualquier compu 

# print("Current working directory:", os.getcwd(), '\n')
# print("Files in current directory:", os.listdir(), '\n')


""" Lectura de csv's y acceso a SQL"""

#Leo el csv de Bibliotecas Populares y establecimientos

df_bp = pd.read_csv("bibliotecas-populares.csv")
df_ee = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
df_padron = pd.read_csv("padron_poblacion.csv")



con = duckdb.connect()
con.register("Bibliotecas", df_bp)
con.register("Establecimientos", df_ee)
con.register("Padron", df_padron)


""" Testing """

# Estoy mirando por arriba los datos

# Veo que las columnas todas tengan el mismo dato

res = con.execute("""
SELECT COUNT(*) 
FROM Bibliotecas

""").fetchdf()


#print(res) #Son 1902 datos, si testeamos en los otros es igual

# Aca les dejo si quieren ver mas a fondo las columnas de los csv con los tipos de datos, nulls y demas 
# Hay una banda de datos al pedo

# print("Info sobre Bp:")
# print(df_bp.info(), '\n')  

# print("Info sobre Ee:")
# print(df_ee.info(), '\n')

# print("Info sobre padron:")
# print(df_padron.info())




# df_bp["nro_conabip"].nunique()




""" Testeo + """

# Cantidad de Departamentos en EE

cant_DEP_EE = con.execute("""
SELECT COUNT(DISTINCT "Código de departamento") AS cant_departamentos_ee
FROM Establecimientos
""").fetchdf()

#print(cant_DEP_EE) # 538

# Cantidad de Departamentos en BP

cant_DEP_BP = con.execute("""
SELECT COUNT(DISTINCT id_departamento) AS cant_departamentos_bp
FROM Bibliotecas
""").fetchdf()

#print(cant_DEP_BP)  #437


# Consulta para verificar coincidencias por departamento

consultas = con.execute("""
SELECT 
    ee."Código de departamento" AS codigo_ee,
    bp.id_departamento AS codigo_bp,
    COUNT(*) AS cantidad_coincidencias
FROM Establecimientos ee
JOIN Bibliotecas bp 
    ON ee."Código de departamento" = bp.id_departamento
GROUP BY ee."Código de departamento", bp.id_departamento
""").fetchdf()

#print(consultas) # 435 nos faltarian dos que son medio innecesarios

# cosnulta para ver si hay departamentos con nombres de provincia en BP

mismo_nombre_BP = con.execute("""
SELECT DISTINCT departamento
FROM Bibliotecas AS bp
JOIN (
    SELECT DISTINCT provincia
    FROM Bibliotecas
) provs
ON bp.departamento = provs.provincia
""").fetchdf()

# print(mismo_nombre_BP) # Formosa y Ciudad Autonoma de Buenos Aires (esta es rara)



# Consulta para sumar los casos

# casos_totales_padron = con.execute("""
# SELECT SUM(CAST("Casos" AS INTEGER)) AS total_casos
# FROM Padron
# WHERE TRY_CAST("Casos" AS INTEGER) IS NOT NULL
# """).fetchdf()


# print(casos_totales_padron)








