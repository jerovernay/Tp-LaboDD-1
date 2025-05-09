import duckdb
import pandas as pd
import os

os.chdir(r"C:\Users\Dell\Escritorio\Tp-LaboDD-1\Descargas") # Aca pongan el directorio que esten usando, asi con cambiar esto solo ya nos core en cualquier compu 
print("Current working directory:", os.getcwd(), '\n')
print("Files in current directory:", os.listdir(), '\n')


#Leo el csv de Bibliotecas Populares
bibliotecas = pd.read_csv("bibliotecas-populares.csv")

con = duckdb.connect()
con.register("Bibliotecas", bibliotecas)

establecimientos_ed = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
padron = pd.read_csv("padron_poblacion.csv", dtype={'Area': str})



# Estoy mirando por arriba los datos

# Veo que las columnas todas tengan el mismo dato

res = con.execute("""
SELECT DISTINCT fuente, COUNT(*) as cantidad 
FROM Bibliotecas
GROUP by fuente
""").fetchdf()


print(res) #Son 1902 datos, si testeamos en los otros es igual

# Aca les dejo si quieren ver mas a fondo las columnas de los csv con los tipos de datos, nulls y demas 
# Hay una banda de datos al pedo
print("Info sobre Bp:")
print(bibliotecas.info())  

print("Info sobre Ee:")
print(establecimientos_ed.info())

print("Info sobre padron:")
print(padron.info())

consultaSQL = """
               SELECT Comuna, Area, SUM(CAST(Casos AS DECIMAL)) AS Total_Casos
               FROM padron
               WHERE Area LIKE '02%'
               GROUP BY Comuna, Area
               ORDER BY Comuna;
              """

dataframeResultado = duckdb.query(consultaSQL).df()

print(dataframeResultado)

''' Dejo comentado esta parte que estaba en la ultima version del archivo porque es posible que los nombres no se correspondan con la nueva version

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
'''


