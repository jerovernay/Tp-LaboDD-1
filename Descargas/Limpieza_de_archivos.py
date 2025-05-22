import pandas as pd
import duckdb
import numpy as np
import os

os.chdir(r"C:\Users\Dell\Escritorio")       # Por favor completar con el path donde se encuentre la carpeta Tp-LaboDD-1

# Leemos los archivos originales:
EE = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\EE(2da_opcion).csv", sep=';')               # Usamos una version reducida en formato csv ya que el excel original 
Padron = pd.read_csv(r'Tp-LaboDD-1\Descargas\TablasOriginales\padron_poblacion.csv')                  # contiene mucha informacion irrelevante.
bibliotecas = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\bibliotecas_populares.csv")
establecimientos_ed = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\2025.04.08_padron_oficial_establecimientos_educativos_die.csv", dtype={"id_departamento":str})

""" Primero limpiamos EE en base al original """
seen = set()
mascara = []

for col in EE.columns:
    
    if col not in seen:
        mascara.append(True)
        seen.add(col)
    else:
        mascara.append(False)

EE_aux = EE.loc[:, mascara]

# Limpiamos espacios en los nombres de columnas
EE_aux.columns = EE_aux.columns.str.strip()

# Seleccionamos solo las deseadas (dejamos jurisdiccion, porque asi podemos entender bien el codigo,id, area de departamento)
EE_aux = EE_aux.reset_index()[['Código de departamento','Cueanexo','Departamento', 'Común', 'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 'Secundario - INET']]

# Limpiamos la antartida
EE_aux = EE_aux[EE_aux['Código de departamento'] != 94028]

#Renombre de la columna "Código de departamento" a "id_departamento"
EE_aux.rename(columns={"Código de departamento" : "id_departamento"}, inplace = True)


# -----------------------------------------------------------

#Rellenamos todos los NaN de la columna "Primario" con 0
EE_aux['Primario'] = np.ceil(EE_aux['Primario'].fillna(0))


# #Creamos y redondeamos Jardin, tomando 1 si el EE tiene Jardin maternal o Jardin de infantes, y 0 en su defecto
EE_aux['Jardin'] = np.ceil(
    EE_aux[['Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes']]
    .mean(axis=1)
    .fillna(0)
)


# #Creamos y redondeamos Secundario,  tomando 1 si el EE tiene Secundario o Secundario - INET, y 0 en su defecto
EE_aux['Secun'] = np.ceil(
    EE_aux[['Secundario', 'Secundario - INET']]
    .mean(axis=1)
    .fillna(0)
)

# # Eliminamos columnas originales y renombramos "Secun"
columnas_a_eliminar = [
    'Nivel inicial - Jardín maternal',
    'Nivel inicial - Jardín de infantes',
    'Secundario', 
    'Secundario - INET'
]

EE_nuevo = EE_aux.drop(columns=columnas_a_eliminar)
EE_nuevo = EE_nuevo.rename(columns={'Secun': 'Secundario'})

# Convertimos a csv:
#EE_nuevo.to_csv('EE_limpio.csv', index=False)


""" Limpiamos Padron en base al original """

#Buscamos el padrón de la Comuna 15, y reemplazamos su id_departamento. Pasa de 2015 a 2115
Padron.loc[Padron['departamento'] == 'COMUNA 15', 'id_departamento'] = Padron.loc[Padron['departamento'] == 'COMUNA 15', 'id_departamento'].replace(2105, 2115)

#Definimos una función para clasificar por grupos etarios
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
    
# Quitamos la comuna porque no suma en nada y perdemos datos a la hora de agruparla asi.
# Todos los departamentos que tengan un numero como nombre son leidos como comunas, mientras que el resto ni los lee
# Yo creo que va a convenir dejar los nombres, pero aca ya tenemos un approach


#Aplicamos la función para clasificar por grupo etario
Padron['Rango etario'] = Padron['edad'].apply(clasificacion_por_grupo_etario_padron)


Padron = Padron[Padron['Rango etario'].notna()]

Padron = Padron.groupby(['id_departamento','Rango etario']).agg({
    'casos' : 'sum',
    })

# Calculamos el total de casos por Area para recalcular los porcentajes

Padron = Padron.reset_index()[['id_departamento', 'Rango etario', 'casos']]

# Testeamos el nuevo padron

espacios = []

for comuna, grupo in Padron.groupby('id_departamento'):
    espacios.append(grupo)  # agregamos el bloque original


Padron = pd.concat(espacios, ignore_index= True)


# Reemplazamos los id_departamento de CABA y de Ushuaia

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

#Definimos la lista única de id_departamentos
deptos = list(Padron['id_departamento'].unique())

#Definimos una lista de los rangos etarios por orden
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

#Establecemos un criterio para ordenar los rangos etarios, basado en la lista definida previamente.
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
    
    # Agrega dos filas vacías (excepto después del último departamento), para mayor prolijidad
    if depto != deptos[-1]:
        dfs_ordenados.append(pd.DataFrame([{}] * 2))  # Filas vacías

# Concatena todos los DataFrames
df_final = pd.concat(dfs_ordenados, ignore_index=True)

# Resetea índice
df_final.reset_index(drop=True, inplace=True)

# Convertimos a csv: 
#df_final.to_csv('Padron_limpio.csv', index=False) 



""" Limpiamos BP en base al original """

bibliotecas = bibliotecas.reset_index()[['id_departamento', 'nro_conabip', 'provincia', 'fecha_fundacion', 'mail']]

# Corregimos chascomus

bibliotecas['id_departamento'] = bibliotecas['id_departamento'].replace({
    6217 : 6218
    })

# Vamos a hacer la corrección a mano de las columnas, porque son bastante pocas y creemos mas rápido resolverlo
# de esta manera que automatizándolo.

# Convertimos a csv:
#bibliotecas.to_csv("BP_limpio.csv", index = False)

""" Generamos Departamento """

consulta_departamento = """
                        SELECT DISTINCT id_departamento, departamento, substr(CAST(id_departamento AS VARCHAR), 1, 2) AS id_provincia
                        FROM establecimientos_ed
                    """
departamentos = duckdb.query(consulta_departamento).df()

#Reemplazamos los id_departamento para que coincidan con los de EE
departamentos['id_departamento'] = departamentos['id_departamento'].replace({
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
})

#Modificamos el id de la Comuna 15, que nos quedaba coincidiendo con el de la Comuna 5
departamentos.loc[departamentos['Departamento'] == 'Comuna 15', 'id_departamento'] = departamentos.loc[departamentos['Departamento'] == 'Comuna 15', 'id_departamento'].replace(2105, 2115)

# Convertimos a csv:
#departamentos.to_csv('Departamento.csv', index=False)

""" Generamos Provincia """

consulta_provincia = """
                        SELECT DISTINCT SUBSTR(id_departamento, 1, 2) AS id,"Jurisdicción" AS nombre
                        FROM establecimientos_ed
                    """

                    
provincia = duckdb.query(consulta_provincia).df()

# Convertimos a csv:
#provincia.to_csv('Provincia.csv', index=False)