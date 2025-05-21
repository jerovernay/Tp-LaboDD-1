# codigos de limpieza para padron, usando el archivo corregido para la columna 15
import pandas as pd

Padron = pd.read_csv(r'C:\Users\gasto\Downloads\padron_poblacion.csv')

Padron.loc[Padron['departamento'] == 'COMUNA 15', 'id_departamento'] = Padron.loc[Padron['departamento'] == 'COMUNA 15', 'id_departamento'].replace(2105, 2115)

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
        return "> 60"
    
# Quito la comuna porque no suma en nada y perdemos datos a la hora de agruparla asi.
# Todos los departamentos que tengan un numero como nombre son leidos como comunas, mientras que el resto ni los lee
# Yo creo que va a convenir dejar los nombres, pero aca ya tenemos un approach



Padron['Rango etario'] = Padron['edad'].apply(clasificacion_por_grupo_etario_padron)

Padron = Padron[Padron['Rango etario'].notna()]

Padron = Padron.groupby(['id_departamento','Rango etario']).agg({
    'casos' : 'sum',
    })

# Calculamos el total de casos por Area para recalcular los porcentajes

Padron = Padron.reset_index()[['id_departamento', 'Rango etario', 'casos']]

# Testeo el nuevo padron

espacios = []

for comuna, grupo in Padron.groupby('id_departamento'):
    espacios.append(grupo)  # agregamos el bloque original


Padron = pd.concat(espacios, ignore_index= True)


# Reemplazamos los id_departamento de comunas y de Ushuaia

print(Padron['id_departamento'].dtype)

Padron['id_departamento'] = Padron['id_departamento'].replace({
    94015: 94014,
    94008: 94007,
    2007: 2101,
    2014: 2102,
    2021: 2103,
    2028: 2104,
    2035: 2105,
    2042: 2106,
    2049: 2107,
    2056: 2108,
    2063: 2109,
    2070: 2110,
    2077: 2111,
    2084: 2112,
    2091: 2113,
    2098: 2114,
    2105: 2115
})


# -------------------------------------------------------


deptos = list(Padron['id_departamento'].unique())


orden_rangos = [
    "0 a 2",
    "3 a 5",
    "6 a 11",
    "12 a 18",
    "19 a 29",
    "30 a 45",
    "46 a 60",
    "> 60"
]

Padron['Rango etario'] = pd.Categorical(
    Padron['Rango etario'],
    categories=orden_rangos,
    ordered=True
)

dfs_ordenados = []

for depto in deptos:
    # Filtra datos del departamento actual
    df_depto = Padron[Padron['id_departamento'] == depto].copy()
    
    # Ordena por rango etario (usando el orden categórico)
    df_depto = df_depto.sort_values('Rango etario')
    
    
    # Agrega al listado
    dfs_ordenados.append(df_depto)
    
    # Agrega dos filas vacías (excepto después del último departamento)
    if depto != deptos[-1]:
        dfs_ordenados.append(pd.DataFrame([{}] * 2))  # Filas vacías

# Concatena todos los DataFrames
df_final = pd.concat(dfs_ordenados, ignore_index=True)

# Resetea índice
df_final.reset_index(drop=True, inplace=True)

df_final.to_csv('Padron_limpio_final.csv', index=False) 
