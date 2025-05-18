import numpy as np
import pandas as pd
EE = pd.read_csv(r"C:\Users\gasto\Downloads\EE_limpio.csv")


EE['Primario'] = np.ceil(EE['Primario'].fillna(0))

#Creamos y redondeamos Jardin
EE['Jardin'] = np.ceil(
    EE[['Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes']]
    .mean(axis=1)
    .fillna(0)
)

#Creamos y redondeamos Secundario
EE['Secun'] = np.ceil(
    EE[['Secundario', 'Secundario - INET']]
    .mean(axis=1)
    .fillna(0)
)

# Eliminamos columnas originales y renombramos
columnas_a_eliminar = [
    'Nivel inicial - Jardín maternal',
    'Nivel inicial - Jardín de infantes',
    'Secundario', 
    'Secundario - INET'
]

EE_nuevo = EE.drop(columns=columnas_a_eliminar)
EE_nuevo = EE_nuevo.rename(columns={'Secun': 'Secundario'})

EE_nuevo.to_csv('EE_limpio_2.csv', index=False)

