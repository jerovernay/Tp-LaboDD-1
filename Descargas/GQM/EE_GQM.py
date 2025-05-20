import duckdb
import pandas as pd


establecimientos_ed = pd.read_csv("2025.04.08_padron_oficial_establecimientos_educativos_die.csv", sep=';')

con = duckdb.connect()

con.register("EE", establecimientos_ed)

print("Veamos los datos en crudo: ")
establecimientos_ed.info()
print('\n Nulls en columnas: ')
print(establecimientos_ed.isnull().sum(), '\n')


""" Problemas con los Mails """

Registros_totales = len(establecimientos_ed)
mails_nulls = establecimientos_ed["Mail"].isnull().sum()
completitud_mails = mails_nulls / Registros_totales * 100

print("\nPorcentaje de establecimientos sin correo electronico: ")
print(round(completitud_mails,2), '\n')



mails_mal_formato_total = """
SELECT 
    COUNT(*) * 100.0 / (
    SELECT COUNT(*) FROM EE WHERE "Mail" IS NOT NULL
    ) AS porcentaje_mail_totalmente_invalidos
FROM EE
WHERE "Mail" IS NOT NULL
  AND INSTR("Mail", '@') = 0;
"""


cant_mails_mal_totalmente = con.query(mails_mal_formato_total).to_df()
print(cant_mails_mal_totalmente, '\n') 

""" Calculamos el porcentaje de mails validos """

# Teniendo en cuenta que los mails son 81.71 validos, 
# le multiplicamos el porcentaje que tiene al menos un arroba 

porcentaje_EE_mail_valido = (99.65 * 81.71 / 100)
print(porcentaje_EE_mail_valido, '\n') # 81.42 





""" Problemas con el domicilio """

# Porcentaje de domicilios sin numero 

sin_numero = establecimientos_ed["Domicilio"].apply(lambda x: not any(char.isdigit() for char in str(x)))
porcentaje_sin_numero = sin_numero.mean() * 100

print("\nPorcentaje de Domicilios sin Numero: ")
print(round(porcentaje_sin_numero, 3), '\n') # 26,12


# Porcentaje de domicilios con sintexis invalida("falsos")

chars_invalidos = {'?', '%', '$', '#', '@', '*', ';','^', '!', '<', '>'}
patron_invalido = {'', ' '}

def es_invalido(x):
    
    x_str = str(x).strip().lower()      #Normalizacion del String
    
    return(x_str in patron_invalido or  any(y in str(x) for y in chars_invalidos))


invalidos = establecimientos_ed["Domicilio"].apply(es_invalido)
porcentaje_invalido = invalidos.mean() * 100

print("\nPorcentaje de Domicilios Invalidos: ")
print(round(porcentaje_invalido, 3), '\n') # 1,14%

porcentaje_problema_domicilio_general = (73.88 * 98.84/ 100)
print(round(porcentaje_problema_domicilio_general, 3), '\n') # 73.02%






















