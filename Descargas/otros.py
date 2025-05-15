import duckdb
import pandas as pd
import os

# print("Current working directory:", os.getcwd(), '\n')
# print("Files in current directory:", os.listdir(), '\n')


#Leo el csv de Bibliotecas Populares
bibliotecas = pd.read_csv("bibliotecas-populares.csv")
establecimientos_ed = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
padron = pd.read_csv("padron_poblacion.csv", dtype={'Area': str})


# Tenemos que clasificar el padron en grupos etarios para asi despues hacer las consultas que nos piden respecto a esto


def clasificacion_por_grupo_etario_padron(edad):
    
    if 0 <= edad <= 2:
        return "0 a 2"
    elif 3 <= edad <= 5:
        return '3 a 5'
    elif 6 <= edad <= 11:
        return "6 a 11"
    elif 12 <= edad <= 18:
        return '12 a 18'
    elif 19 <= edad <= 29:
        return "19 a 29"
    elif 30 <= edad <= 45:
        return '30 a 45'
    elif 46 <= edad <= 60:
        return '46 a 60'
    else:
        return "< 60"
    
# Quito la comuna porque no suma en nada y perdemos datos a la hora de agruparla asi.
# Todos los departamentos que tengan un numero como nombre son leidos como comunas, mientras que el resto ni los lee
# Yo creo que va a convenir dejar los nombres, pero aca ya tenemos un approach

padron01 = padron.copy()

padron01['Rango etario'] = padron01['Edad'].apply(clasificacion_por_grupo_etario_padron)

padron02 = padron01[padron01['Rango etario'].notna()]

padron03 = padron02.groupby(['Area','Rango etario']).agg({
    'Casos' : 'sum',
    })

# Calculamos el total de casos por Area para recalcular los porcentajes


padron03['%'] = padron03.groupby(['Area'])['Casos'].transform(lambda x: (x / x.sum()) * 100)

padron03['Acumulado %'] = padron03.groupby(['Area'])['%'].cumsum()

padron03 = padron03.reset_index()[['Rango etario', 'Casos', '%', 'Acumulado %', 'Area']]

# Testeo el nuevo padron

espacios = []

# Pongo espacios entre las columnas, asi no queda todo junto

for comuna, grupo in padron03.groupby('Area'):
    espacios.append(grupo)  # agregamos el bloque original
    espacios.append(pd.DataFrame([[''] * len(grupo.columns)] * 2, columns=grupo.columns))  # agregamos 2 filas vacías

padron_final = pd.concat(espacios, ignore_index= True)

# Genera el padron final, lo comento asi no lo creamos devuelta, xlas.

# padron_final.to_csv("padron_prueba02.csv", index=False)


''' Limpio en base a nuestros valores elegidos el csv de BP'''

biblio01 = bibliotecas.copy()

biblio01 = biblio01.reset_index()[['id_provincia', 'id_departamento', 'departamento', 'fecha_fundacion', 'nro_conabip', 'mail', 'cod_localidad']]

#biblio01.to_csv("test_biblio.csv", index = False)



''' Limpio en base a nuestros valores elegidos el csv de EE'''

establecimientos01 = establecimientos_ed.copy()

# Eliminar columnas duplicadas (conservar solo primera aparición)
seen = set()
mascara = []
for col in establecimientos01.columns:
    if col not in seen:
        mascara.append(True)
        seen.add(col)
    else:
        mascara.append(False)

establecimientos01 = establecimientos01.loc[:, mascara]

# Limpiar espacios en los nombres de columnas
establecimientos01.columns = establecimientos01.columns.str.strip()

# Mostrar columnas para verificar
print(establecimientos01.columns.tolist())

# Seleccionar solo las deseada, deje jurisdiccion, porque asi podemos entender bien el codigo,id, area de departamento
establecimientos01 = establecimientos01.reset_index()[['Cueanexo','Jurisdicción','Código de departamento', 'Común', 'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario']]

#establecimientos01.to_csv("EE_prueba4.csv", index = False)