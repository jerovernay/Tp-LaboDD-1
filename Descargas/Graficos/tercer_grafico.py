import pandas as pd
import seaborn as sns
import duckdb
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.ticker import ScalarFormatter

EE = pd.read_csv(r"C:\Users\gasto\Downloads\labo\EE_limpio _grafico.csv")
BP = pd.read_csv(r"C:\Users\gasto\Downloads\labo\BP_limpio_grafico.csv")
gr = pd.read_csv(r"C:\Users\gasto\Downloads\labo\graf_3.csv")

con = duckdb.connect()

con.register('EE', EE)
con.register('BP', BP)  

query = """
    SELECT Departamento, provincia, COUNT(Departamento) AS cantidad 
    FROM EE JOIN BP ON EE."id_departamento" = BP."id_departamento"
    GROUP BY Departamento, provincia
    ORDER BY cantidad DESC
"""

resultado = con.execute(query).fetchdf()
resultado.to_csv('graf_3.csv', index=False)

#ordena por mediana de provincia, de manera descendiente
provincia_orden = (
    gr.groupby("provincia")["cantidad"]
    .median()
    .sort_values(ascending=False)
    .index
)

#tamaño del grafico
plt.figure(figsize=(16,10))

# --- Boxplot ---
boxplot = sns.boxplot(
    data=gr,
    x="provincia",
    y="cantidad",
    order=provincia_orden,
    palette="colorblind",
    width=0.6,
    showfliers=False,  # Ocultamos outliers (los mostramos con stripplot)
    linewidth=1.5
)

# --- Boxplot ---
sns.stripplot(
    data=gr,
    x="provincia",
    y="cantidad",
    order=provincia_orden,       # Agrupar por provincia
    palette="colorblind",     # Misma paleta que boxplot
    dodge=False,           
    alpha=1,
    size=5,
    linewidth=0.5,
    legend=False           # Oculta leyenda duplicada
)

# para la escala del eje y
plt.yscale('log')
ax = plt.gca()
ax.yaxis.set_major_formatter(ScalarFormatter())
ax.ticklabel_format(style='plain', axis='y')


# --- Ajustes finales ---
plt.title("Distribución de EE por provincia", fontsize=14)
plt.xlabel("Provincia", fontsize=14)
plt.ylabel("Cantidad de EE", fontsize=14)

plt.xticks(rotation=45, ha="right", fontsize=14)

#lineas para que se entiende a que provincia pertenece cada boxplot
for i in range(len(provincia_orden)):
    plt.axvline(i, color='gray', linewidth=0.3, linestyle='--')
    
plt.show()
