import pandas as pd


# Leo los csv originales 
bibliotecas = pd.read_csv("bibliotecas-populares.csv")
establecimientos_ed = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
padron = pd.read_csv("padron_poblacion.csv", dtype={'Area': str})




""" Limpio detalles de EE en base al original """

establecimientos_ed.rename(columns={"Código de departamento" : "id_departamento"}, inplace = True)

# Eliminar columnas duplicadas (conservar solo primera aparición) 
# Nuestro objetivo es quedarnos solo con las de modalidad comun, que son las que se adecuan a lo que nos piden

seen = set()
mascara = []

for col in establecimientos_ed.columns:
    
    if col not in seen:
        mascara.append(True)
        seen.add(col)
    else:
        mascara.append(False)

establecimientos_ed = establecimientos_ed.loc[:, mascara]

# Limpiar espacios en los nombres de columnas
establecimientos_ed.columns = establecimientos_ed.columns.str.strip()

# Seleccionar solo las deseada, deje jurisdiccion, porque asi podemos entender bien el codigo,id, area de departamento
establecimientos_ed = establecimientos_ed.reset_index()[['id_departamento','Cueanexo','Departamento', 'Común', 'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 'Secundario - INET']]

# Limpio la antartida
establecimientos_ed = establecimientos_ed[establecimientos_ed['id_departamento'] != 94028]

# Creamos el csv limpio
# establecimientos_ed.to_csv("EE_limpio.csv", index = False)



""" Limpio Padron en base al original """

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



padron['Rango etario'] = padron['Edad'].apply(clasificacion_por_grupo_etario_padron)

padron = padron[padron['Rango etario'].notna()]

padron = padron.groupby(['Area','Rango etario']).agg({
    'Casos' : 'sum',
    })

# Calculamos el total de casos por Area para recalcular los porcentajes


padron['%'] = padron.groupby(['Area'])['Casos'].transform(lambda x: (x / x.sum()) * 100)

padron['Acumulado %'] = padron.groupby(['Area'])['%'].cumsum()

padron = padron.reset_index()[['Area', 'Rango etario', 'Casos', '%', 'Acumulado %']]

# Testeo el nuevo padron

espacios = []

# Pongo espacios entre las columnas, asi no queda todo junto

for comuna, grupo in padron.groupby('Area'):
    espacios.append(grupo)  # agregamos el bloque original
    espacios.append(pd.DataFrame([[''] * len(grupo.columns)] * 2, columns=grupo.columns))  # agregamos 2 filas vacías

padron_final = pd.concat(espacios, ignore_index= True)

padron_final.rename(columns={'Area': 'id_departamento'}, inplace=True)

# Reemplazamos los id_departamento de comunas y de Ushuaia

print(padron_final['id_departamento'].dtype)

padron_final['id_departamento'] = padron_final['id_departamento'].replace({
    '94015': '94014',
    '94008': '94007',
    '02007': '02101',
    '02014': '02102',
    '02021': '02103',
    '02028': '02104',
    '02035': '02105',
    '02042': '02106',
    '02049': '02107',
    '02056': '02108',
    '02063': '02109',
    '02070': '02110',
    '02077': '02111',
    '02084': '02112',
    '02091': '02113',
    '02098': '02114'
})


# Genera el padron final, lo comento asi no salta error

#padron_final.to_csv("padron_limpio.csv", index=False)



""" Limpio bibliotecas en base a la original """

bibliotecas = bibliotecas.reset_index()[['id_departamento', 'nro_conabip', 'provincia', 'fecha_fundacion', 'mail']]

# Corrigo chascomus
print(bibliotecas['id_departamento'].dtype)

bibliotecas['id_departamento'] = bibliotecas['id_departamento'].replace({
    6217 : 6218
    })

# Vamos a hacer la correccion a mano de las columnas, porque son bastante pocas y creemos mas rapido resolverlo
# de esta manera que automatizandolo.


#bibliotecas.to_csv("BP_limpio.csv", index = False)





