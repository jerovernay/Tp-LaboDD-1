import duckdb
import pandas as pd
import os

print("Current working directory:", os.getcwd(), '\n')
print("Files in current directory:", os.listdir(), '\n')


#Leo el csv de Bibliotecas Populares
df = pd.read_csv("bibliotecas-populares.csv")

con = duckdb.connect()
con.register("Bibliotecas", df)


# Estoy mirando por arriba los datos

# Veo que las columnas todas tengan el mismo dato

res = con.execute("""
SELECT DISTINCT fuente, COUNT(*) as cantidad 
FROM Bibliotecas
GROUP by fuente
""").fetchdf()


print(res) #Son 1902 datos, si testeamos en los otros es igual





