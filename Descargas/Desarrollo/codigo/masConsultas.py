import duckdb
import pandas as pd

# Leo los csv's
bibliotecas = pd.read_csv("bibliotecas-populares.csv")
establecimientos_ed = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
padron = pd.read_csv("padron_poblacion.csv", dtype={'Area': str})

padron_filtrado = pd.read_csv("padron_limpio.csv", skip_blank_lines= True) # no lee los espacios de entre medio
biblio_filtrado = pd.read_csv("BP_limpio.csv")
establecimientos_ed_filtrado = pd.read_csv("EE_limpio.csv")


# conecto a duck para hacer consultas
con = duckdb.connect()

con.register("BP", biblio_filtrado)
con.register("EE", establecimientos_ed_filtrado)
con.register("Padron", padron_filtrado)


# Departamentos en Padron que no están en EE
no_en_ee = con.query("""
    SELECT DISTINCT Padron.id_departamento AS dept_no_en_ee
    FROM Padron
    LEFT JOIN EE
      ON Padron.id_departamento = EE."id_departamento"
    WHERE EE."id_departamento" IS NULL;
""").to_df()



# Departamentos en EE que no están en Padron
no_en_padron = con.query("""
    SELECT DISTINCT EE."id_departamento" AS dept_no_en_padron
    FROM EE
    LEFT JOIN Padron
      ON EE."id_departamento" = Padron.id_departamento
    WHERE Padron.id_departamento IS NULL;
""").to_df()


print("Departamentos en EE que NO están en Padrón:")
print(no_en_padron, '\n')

print("Departamentos en Padrón que NO están en EE:")
print(no_en_ee, '\n')

# Busco los nulls

null_departamentos = con.query("""
    SELECT *
    FROM Padron
    WHERE id_departamento IS NULL;
""").to_df()

print(null_departamentos)


""" Esto ocurre porque dejamos un espacio en el medio, lo que nos facilita la visualizacion pero genera nulls """

# Resuelvo el caso de 94028 = Antartida Argentina

conteo_94028 = con.query("""
    SELECT COUNT(*) AS cantidad
    FROM EE
    WHERE id_departamento = 94028;
""").to_df()

print(conteo_94028) # 1 

""" Borramos esa fila, para tener solo los datos que precisamos, en el padron no aparece nada relacionado 
    a la antartida y como el EE esta ahi, realmente no va a influir en nada el borrarlo """








