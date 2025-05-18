import math
import pandas as pd
EE = pd.read_csv(r"C:\Users\gasto\Downloads\EE_limpio.csv")

EE['Primario'] = EE['Primario'].fillna(0).apply(math.ceil)

EE['Jardin'] = (EE['Nivel inicial - Jardín maternal'] + EE['Nivel inicial - Jardín de infantes'])/2
EE['Jardin'] = EE['Jardin'].fillna(0).apply(math.ceil)


EE['Secun'] = (EE['Secundario'] + EE['Secundario - INET'])/2
EE['Secun'] = EE['Secun'].fillna(0).apply(math.ceil)


EE_nuevo = EE.drop(columns=['Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Secundario', 'Secundario - INET'])
EE_nuevo.rename(columns = {'Secun' : 'Secundario'}, inplace=True)


EE_nuevo.to_csv('EE_limpio_2.csv', index=False)
