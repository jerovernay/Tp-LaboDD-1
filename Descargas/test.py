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



