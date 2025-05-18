import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

BP = pd.read_csv(r"C:\Users\gasto\Downloads\BP_limpio.csv")
EE = pd.read_csv(r"C:\Users\gasto\Downloads\EE_limpio.csv")
Padron = pd.read_csv(r"C:\Users\gasto\Downloads\labo\padron_limpio.csv")

provincias = BP['provincia'].unique()

diccionario = {}
for provincia in provincias:
    diccionario[provincia] = len(BP.loc[BP['provincia'] == provincia])

# Creamos listas separadas para cada columna
provincias = list(diccionario.keys())
cantidad = list(diccionario.values())

# DataFrame con columnas
df = pd.DataFrame({
    'Provincia': provincias,
    'Cantidad': cantidad
})

df = df.sort_values(by='Cantidad', ascending=False) #ordeno las provincias por cantidad de bibliotecas de manera decreciente

sns.set(
    rc = {"figure.figsize": (15, 6)}
)  # Configuracion del ancho y largo del grafico para que se vea mejor

ax = sns.barplot(data=df, x='Provincia', y='Cantidad', palette='plasma') #grafico

plt.xticks(rotation=45, ha="right")  # Rotación y alineación del texto para que se puedan ver el nombre de cada una de las provincias

plt.title("Bibliotecas por provincia") # Titulo del grafico

for i, valor in enumerate(df["Cantidad"]):
    ax.text(i, valor + 0.5, str(valor), ha="center", fontsize=12)  # Usamos esto para poner el numero arriba de cada barra, asi es mas claro

plt.show()

#usamos la paleta plasma, ya que es amigable pora personas con daltonismo  (protanopia y deuteranopia)

