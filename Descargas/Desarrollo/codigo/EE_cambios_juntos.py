#codigos de limpieza para EE

import pandas as pd
import numpy as np


EE = pd.read_csv(r"C:\Users\gasto\Downloads\EE(2da_opcion).csv", sep=';')


seen = set()
mascara = []

for col in EE.columns:
    
    if col not in seen:
        mascara.append(True)
        seen.add(col)
    else:
        mascara.append(False)

establecimientos_ed = EE.loc[:, mascara]

# Limpiar espacios en los nombres de columnas
establecimientos_ed.columns = establecimientos_ed.columns.str.strip()

# Seleccionar solo las deseada, deje jurisdiccion, porque asi podemos entender bien el codigo,id, area de departamento
establecimientos_ed = establecimientos_ed.reset_index()[['Código de departamento','Cueanexo','Departamento', 'Común', 'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 'Secundario - INET']]

# Limpio la antartida
establecimientos_ed = establecimientos_ed[establecimientos_ed['Código de departamento'] != 94028]

establecimientos_ed.rename(columns={"Código de departamento" : "id_departamento"}, inplace = True)


# -----------------------------------------------------------


establecimientos_ed['Primario'] = np.ceil(establecimientos_ed['Primario'].fillna(0))


# #Creamos y redondeamos Jardin
establecimientos_ed['Jardin'] = np.ceil(
    establecimientos_ed[['Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes']]
    .mean(axis=1)
    .fillna(0)
)


# #Creamos y redondeamos Secundario
establecimientos_ed['Secun'] = np.ceil(
    establecimientos_ed[['Secundario', 'Secundario - INET']]
    .mean(axis=1)
    .fillna(0)
)

# # Eliminamos columnas originales y renombramos
columnas_a_eliminar = [
    'Nivel inicial - Jardín maternal',
    'Nivel inicial - Jardín de infantes',
    'Secundario', 
    'Secundario - INET'
]

EE_nuevo = establecimientos_ed.drop(columns=columnas_a_eliminar)
EE_nuevo = EE_nuevo.rename(columns={'Secun': 'Secundario'})

EE_nuevo.to_csv('EE_limpio_final.csv', index=False)
