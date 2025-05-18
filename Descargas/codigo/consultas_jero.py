import duckdb
import pandas as pd



# Leo los csv
bibliotecas = pd.read_csv("bibliotecas-populares.csv")
establecimientos_ed = pd.read_csv("2025.04.08_padroin_oficial_establecimientos_educativos_die.csv", sep=';')
padron = pd.read_csv("padron_poblacion.csv", dtype={'Area': str})
padron_filtrado = pd.read_csv("padron_limpio.csv")
biblio_filtrado = pd.read_csv("test_biblio.csv")
establecimientos_ed_filtrado = pd.read_csv("EE_casi2.csv")


# conecto a duck para hacer consultas
con = duckdb.connect()

con.register("BP", biblio_filtrado)
con.register("EE", establecimientos_ed_filtrado)
con.register("Padron", padron_filtrado)


""" Dado que hice cambios al EE, puede que algunos no funcionen """


# Cantidad de códigos de departamento únicos en EE
consulta_ee_dept_unicos = """
SELECT COUNT(DISTINCT "Código de departamento") AS cant_departamentos_ee
FROM EE;
"""

# Cantidad de códigos de departamento únicos en Padrón
consulta_padron_dept_unicos = """
SELECT COUNT(DISTINCT Area) AS cant_departamentos_padron
FROM Padron;
"""

# Cantidad de codigos unicos de departamento en BP
consulta_bp_dept_unicos ="""
SELECT COUNT(DISTINCT id_departamento) AS cant_departamentos_bp
FROM BP
"""

# Ejecutar las consultas
cant_ee = con.query(consulta_ee_dept_unicos).to_df()
cant_padron = con.query(consulta_padron_dept_unicos).to_df()
cant_bp = con.query(consulta_bp_dept_unicos).to_df()

# Mostrar resultados
# print("Departamentos únicos en EE:")    # 528
# print(cant_ee, '\n')

# print("\nDepartamentos únicos en Padrón:")       # 527
# print(cant_padron, '\n')

# print("Departamentos unicos en BP: ")           # 437 , hay departamentos que no tienen BP
# print(cant_bp)

# Veo cuales son unicos 
consulta_departamentos_en_ambas = """
SELECT COUNT(DISTINCT EE."Código de departamento") AS departamentos_comunes
FROM EE
INNER JOIN Padron
    ON EE."Código de departamento" = Padron.Area;
"""

resultado_comunes = con.query(consulta_departamentos_en_ambas).to_df()
# print("Cantidad de departamentos en común entre EE y Padrón:")       # 511
# print(resultado_comunes, '\n')



# Departamentos en EE que no están en Padron
no_en_padron = con.query("""
    SELECT DISTINCT EE."Código de departamento" AS dept_no_en_padron
    FROM EE
    LEFT JOIN Padron
      ON EE."Código de departamento" = Padron.Area
    WHERE Padron.Area IS NULL;
""").to_df()


# Departamentos en Padron que no están en EE
no_en_ee = con.query("""
    SELECT DISTINCT Padron.id_departamento AS dept_no_en_ee
    FROM Padron
    LEFT JOIN EE
      ON Padron.Area = EE."id_departameto"
    WHERE EE."id_departamento" IS NULL;
""").to_df()

# Mostrar resultados
#print("Departamentos en EE que NO están en Padrón:")
#print(no_en_padron, '\n')

# print("Departamentos en Padrón que NO están en EE:")
# print(no_en_ee)

"Código de departamento"
# confirmacion de lo mismo 

col1 = padron_filtrado['Area']
col2 = establecimientos_ed_filtrado['id_departamento']


valores_col1 = set(col1.dropna().unique())
valores_col2 = set(col2.dropna().unique())

solo_en_col1 = valores_col1 - valores_col2
solo_en_col2 = valores_col2 - valores_col1


# print("\nValores que están en col1 pero no en col2:")
# print(solo_en_col1)

# print("\nValores que están en col2 pero no en col1:")
# print(solo_en_col2)

# Cuantos departamentos tienen los 3 en comun, sabiendo que BP tiene 437 codigos de area unicos

consulta_3_en_comun = """
SELECT COUNT(DISTINCT BP.id_departamento) AS departamentos_comunes_3
FROM BP
INNER JOIN EE
  ON BP.id_departamento = EE."Código de departamento"
INNER JOIN Padron
  ON BP.id_departamento = Padron.Area;
"""

resultado_3 = con.query(consulta_3_en_comun).to_df()
# print("\nCantidad de departamentos presentes en BP, EE y Padron:") # 433
# print(resultado_3)

# consulto los 4 que me faltan

""" 
Primero seleccionamos todos los códigos de BP
Luego quitamos los que están en EE y Padron simultáneamente (con INNER JOIN) 
"""

consulta_4_faltantes = """
SELECT DISTINCT id_departamento
FROM BP
WHERE id_departamento NOT IN (
    SELECT DISTINCT BP.id_departamento
    FROM BP
    INNER JOIN EE ON BP.id_departamento = EE."Código de departamento"
    INNER JOIN Padron ON BP.id_departamento = Padron.Area
)
ORDER BY id_departamento;
"""

faltantes_en_ee_o_padron = con.query(consulta_4_faltantes).to_df()
# print("Departamentos que están en BP pero NO en EE y Padron simultáneamente:")
# print(faltantes_en_ee_o_padron)


# Busco en que fila estan

consulta_dos_que_no_tengo = """
SELECT *
FROM BP
WHERE id_departamento IN (2000, 6217);
"""

fila_dos_dpto = con.query(consulta_dos_que_no_tengo).to_df()
# print("\nDos elementos que estan en BP y no en EE y Padron simultaneamente: ")
# print(fila_dos_dpto)



