# -*- coding: utf-8 -*-
"""
Integrantes:
Gaston Arida 
Sebastian Ferreiro
Jeronimo Vernay

Nota: Los archivos originales fueron modificados para facilitar la lectura de los mismos. Aquí se encuentran
    en formato csv, y con pequeños cambios como nombres de columnas que no afectan a la información 
    concreta de los archivos originales.
"""
import duckdb
import pandas as pd
import os

os.chdir(r"C:\Users\Dell\Escritorio") # Vamos a necesitar que aqui completen con el path donde este guardada la carpeta Tp-LaboDD-1

# Leemos los archivos originales
bibliotecas = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\bibliotecas_populares.csv", dtype={'id_provincia': str, 'id_departamento': str})
establecimientos_ed = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\2025.04.08_padron_oficial_establecimientos_educativos_die.csv", dtype={'id_departamento': str})
padron = pd.read_csv(r"Tp-LaboDD-1\Descargas\TablasOriginales\padron_poblacion.csv", dtype={'id_departamento': str})

#EE_limpio para poder crear la tabla Departamento
EE_limpio01 = pd.read_csv("EE_limpio_final_usando2daopcion.csv")


# Generamos las tablas del modelo relacional 
consulta_provincia = """
                        SELECT DISTINCT SUBSTR(id_departamento, 1, 2) AS id, Jurisdicción AS nombre
                        FROM establecimientos_ed
                    """
provincia = duckdb.query(consulta_provincia).df()

consulta_departamento = """
                        SELECT DISTINCT id_departamento, departamento, SUBSTR(id_departamento, 1, 2) AS id_provincia
                        FROM EE_limpio01
                    """
departamento = duckdb.query(consulta_departamento).df()





# duck lee las cosas desde aca, hay que tener bien los que usamos
BP_limpio = pd.read_csv("BP_limpio.csv")
EE_limpio01 = pd.read_csv("EE_limpio_final_usando2daopcion.csv")
Provincia = pd.read_csv("Provincia.csv")
Depto = pd.read_csv("Departamento_corregido.csv")
Poblacion = pd.read_csv("Padron_limpio_final.csv")





"""                                 ====== CONSULTAS SQL =====                                          """

# Registramos los nombres para hacer las consultas

duckdb.register("EE", EE_limpio01)
duckdb.register("Poblacion", Poblacion)
duckdb.register("Departamento", Depto)
duckdb.register("Provincia", Provincia)  
duckdb.register("BP", BP_limpio)



'=== Consulta 1 ==='
'''
Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de EE de cada nivel educativo, considerando solamente la
modalidad común, y la cantidad de habitantes por edad según los niveles
educativos. El orden del reporte debe ser alfabético por provincia y dentro de
las provincias, descendente por cantidad de escuelas primarias.
'''

# Tenemos problemas a la hora de contar a las personas, ya que nos da un numero irreal.
# Si un departamento tiene varios EE y BP, entonces si hacemos una consulta tradicional, multiplica los 
# 8 rango etarios por la cant. BP y cant. EE que hay en dicho departamento (8 x BP x EE) dando un numero irreal

# Solucionamos dicho problema, que se repite en la consulta 3, haciendo subconsultas previas que agreguen la 
# informacion primero y luego hacemos los Joins.


consulta1 = """

-- 1) Contamos los EE por tipo y por departamento
-- Utilizamos Cueanexo como identificador unico de cada Establecimiento 
WITH ee_por_departamento AS (
    SELECT 
        id_departamento,
        COUNT(CASE WHEN Jardin > 0 THEN Cueanexo END) AS jardines,
        COUNT(CASE WHEN Primario > 0 THEN Cueanexo END) AS primarias,
        COUNT(CASE WHEN Secundario > 0 THEN Cueanexo END) AS secundarios
    FROM EE
    GROUP BY id_departamento
),

-- 2) Calculo la poblacion por niveles educativos (por ejem: 3 a 5 es Jardin) con ayuda de la separacion
-- hecha por Rango Etario
poblacion_por_departamento AS (
    SELECT 
        id_departamento,
        SUM(CASE WHEN "Rango etario" = '3 a 5' THEN casos ELSE 0 END) AS poblacion_jardin,
        SUM(CASE WHEN "Rango etario" = '6 a 11' THEN casos ELSE 0 END) AS poblacion_primaria,
        SUM(CASE WHEN "Rango etario" = '12 a 18' THEN casos ELSE 0 END) AS poblacion_secundaria
    FROM Poblacion
    GROUP BY id_departamento
)

-- 3) Consulta que une los datos, como se muestra en la tabla de ejemplo
SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,

    COALESCE(ee.jardines, 0) AS jardines,
    COALESCE(pob.poblacion_jardin, 0) AS poblacion_jardin,

    COALESCE(ee.primarias, 0) AS primarias,
    COALESCE(pob.poblacion_primaria, 0) AS poblacion_primaria,

    COALESCE(ee.secundarios, 0) AS secundarios,
    COALESCE(pob.poblacion_secundaria, 0) AS poblacion_secundaria


-- 4) Indicamos los valores que queremos como generales y hacemos LEFT JOIN para incluir todo
-- Ordenamos como pide el enunciado.
FROM Depto dept
JOIN Provincia prov ON dept.id_provincia = prov.id
LEFT JOIN ee_por_departamento ee ON dept.id_departamento = ee.id_departamento
LEFT JOIN poblacion_por_departamento pob ON dept.id_departamento = pob.id_departamento

ORDER BY prov.nombre ASC, primarias DESC;
"""

resultado1 = duckdb.query(consulta1).to_df()
print(f"resultados 1: {resultado1}")




'=== Consulta 2 ==='
'''
Para cada departamento informar la provincia, el nombre del departamento y
la cantidad de BP fundadas desde 1950. El orden del reporte debe ser
alfabético por provincia y dentro de las provincias, descendente por cantidad de BP de dicha capacidad.
'''

consulta2 = """
SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,
    COUNT(*) AS Cant_BP_fundadas_desde_1950     -- Conteo de las BP's que cumplen condicion


-- Hacemos los JOIN's usando el modelo del enunciado
FROM BP bp
JOIN Departamento dept ON bp.id_departamento = dept.id_departamento
JOIN Provincia prov ON dept.id_provincia = prov.id

-- Como las fechas estan indexadas como año-mes-dia, buscamos solamente los primeros 4 numeros
-- fecha_fundacion es dtype object, por lo que convertimos a los primeros 4 chars en numero y lo comparamos con 1950
-- En SQL la primera posicion es 1, no 0.
WHERE 
    TRY_CAST(SUBSTR(bp.fecha_fundacion, 1, 4) AS INTEGER) >= 1950

-- Agrupamos como pide el enunciado 
GROUP BY prov.nombre, dept.Departamento
ORDER BY prov.nombre ASC, Cant_BP_fundadas_desde_1950 DESC;
"""

resultado2 = duckdb.query(consulta2).to_df()
print(f"resultados 2: {resultado2}")




'=== Consulta 3 ==='

''' 
Para cada departamento, indicar provincia, nombre del departamento,
cantidad de BP, cantidad de EE (de modalidad común) y población total.
Ordenar por cantidad EE descendente, cantidad BP descendente, nombre de 
provincia ascendente y nombre de departamento ascendente. No omitir casos sin BP o EE.
'''

consulta3 = """
-- Creamos las Tablas temporales

-- 1) Contamos las BP unicas por departamento
WITH bp_por_dpto AS (
    SELECT 
        id_departamento,
        COUNT(DISTINCT nro_conabip) AS cantidad_bp      -- Usamos nro_conabip como identificador unico (nuestra PK)
    FROM BP
    GROUP BY id_departamento
),

-- 2) Contamos los EE unico por departamento
ee_por_dpto AS (
    SELECT 
        id_departamento,
        COUNT(DISTINCT Cueanexo) AS cantidad_ee     -- Usamos Cueanexo como identificador unico (nuestra PK)
    FROM EE
    GROUP BY id_departamento
),

-- 3) Al estar dividido en rango etario, sumamos cada "caso" para encontrar el poblacion_total
poblacion_por_dpto AS (
    SELECT 
        id_departamento,
        SUM(casos) AS poblacion_total
    FROM Poblacion
    GROUP BY id_departamento
)

-- 4) Consulta tradicional
SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,
    COALESCE(bp.cantidad_bp, 0) AS cantidad_bp,         -- 0 si no hay BP
    COALESCE(ee.cantidad_ee, 0) AS cantidad_ee,         -- 0 si no hay EE
    COALESCE(pob.poblacion_total, 0) AS poblacion_total 

-- Pedimos el departamento con su provincia y hacemos LEFT JOIN para no excluir los datos que no tengan BP o EE
FROM Departamento dept
JOIN Provincia prov ON dept.id_provincia = prov.id
LEFT JOIN bp_por_dpto bp ON dept.id_departamento = bp.id_departamento
LEFT JOIN ee_por_dpto ee ON dept.id_departamento = ee.id_departamento
LEFT JOIN poblacion_por_dpto pob ON dept.id_departamento = pob.id_departamento

ORDER BY cantidad_ee DESC, cantidad_bp DESC, prov.nombre ASC, dept.Departamento ASC;
"""

resultado3 = duckdb.query(consulta3).to_df()
print(f"resultados 3: {resultado3}")




'=== Consulta 4 ==='
'''
Para cada departamento, indicar provincia, el nombre del departamento y
qué dominios de mail se usan más para las BP
'''


consulta4 = """
SELECT provincia, departamento,
        /* Agaro solo el nombre principal del dominio (sin el .com)) */ 
       SPLIT_PART(dominio_mas_frecuente, '.', 1) AS dominio_principal
FROM (
  SELECT provincia, departamento, dominio AS dominio_mas_frecuente
  FROM (
    SELECT 
      prov.nombre AS provincia,
      dept.Departamento AS departamento,
      SPLIT_PART(bp.mail, '@', 2) AS dominio,   -- Agarro la parte después del @ 
      COUNT(*) AS count,    -- Cuenta repeticiones de cada dominio 
      ROW_NUMBER() OVER (
        PARTITION BY dept.id_departamento  -- Agrupo por departamento 
        ORDER BY COUNT(*) DESC  -- Ordeno dominios de mayor a menor 
      ) AS nro_fila -- La primer fila sera la mas frecuente 
    FROM BP bp
    JOIN Departamento dept ON bp.id_departamento = dept.id_departamento
    JOIN Provincia prov ON dept.id_provincia = prov.id
    WHERE bp.mail IS NOT NULL AND bp.mail LIKE '%@%'                        -- Filtro mails válidos 
    GROUP BY prov.nombre, dept.Departamento, dept.id_departamento, dominio
  ) cierro_parentesis   -- esto es obligatorio para finalizar la subconsulta 
  WHERE nro_fila = 1
) resultado         -- esto tambien es obligatorio para la sintaxis de SQL
"""

resultado4 = duckdb.query(consulta4).to_df()
print(f"resultados 4: {resultado4}")
