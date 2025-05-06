import duckdb
import pandas as pd
import os

os.chdir(r"C:\Users\Dell\Escritorio\Tp-LaboDD-1\Descargas") # Aca pongan el directorio que esten usando, asi con cambiar esto solo ya nos core en cualquier compu 
print("Current working directory:", os.getcwd(), '\n')
print("Files in current directory:", os.listdir(), '\n')


#Leo el csv de Bibliotecas Populares
df_bp = pd.read_csv("bibliotecas-populares.csv")

con = duckdb.connect()
con.register("Bibliotecas", df_bp)

df_ee = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
df_padron = pd.read_csv("padron_poblacion.csv")



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
'''
print("Info sobre Bp:")
print(df_bp.info())  

print("Info sobre Ee:")
print(df_ee.info())

print("Info sobre padron:")
print(df_padron.info())

'''
