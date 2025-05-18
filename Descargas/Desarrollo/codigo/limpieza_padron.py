import pandas as pd
Padron = pd.read_csv(r"C:\Users\gasto\Downloads\labo\Padron_2.csv")

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
    # Filtrar datos del departamento actual
    df_depto = Padron[Padron['id_departamento'] == depto].copy()
    
    # Ordenar por rango etario (usando el orden categórico)
    df_depto = df_depto.sort_values('Rango etario')
    
    df_depto['Acumulado %'] = df_depto['%'].cumsum()
    
    # Agregar al listado
    dfs_ordenados.append(df_depto)
    
    # Agregar dos filas vacías (excepto después del último departamento)
    # if depto != deptos[-1]:
    #     dfs_ordenados.append(pd.DataFrame([{}] * 2))  # Filas vacías

# Concatenar todos los DataFrames
df_final = pd.concat(dfs_ordenados, ignore_index=True)

# Resetear índice
df_final.reset_index(drop=True, inplace=True)

df_final.to_csv('Padron_limpio.csv', index=False) 
