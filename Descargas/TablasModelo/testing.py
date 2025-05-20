import duckdb
import pandas as pd

BP_limpio = pd.read_csv("BP_limpio.csv")
EE_limpio01 = pd.read_csv("EE_limpio_final_usando2daopcion.csv")
Provincia = pd.read_csv("Provincia.csv")
Depto = pd.read_csv("Departamento_corregido.csv")
Poblacion = pd.read_csv("Padron_limpio_final.csv")

# print("Info de Poblacion: ")
# Poblacion.info()
# print("\nInfo de Departamento: ")
# Depto.info()
# print("\nInfo de BP: ")
# BP_limpio.info()
# print("\nInfo de EE: ")
# EE_limpio01.info()

# Cantidad de valores únicos
# print(Poblacion["id_departamento"].nunique())
# print(EE_limpio01["id_departamento"].nunique())

# # ¿Tienen los mismos ID únicos?
# ids_poblacion = set(Poblacion["id_departamento"].unique())
# ids_ee = set(EE_limpio01["id_departamento"].unique())

# print("¿Son exactamente los mismos IDs?", ids_poblacion == ids_ee)
# print("IDs en Poblacion y no en EE:", ids_poblacion - ids_ee)
# print("IDs en EE y no en Poblacion:", ids_ee - ids_poblacion)

duckdb.register("EE", EE_limpio01)
duckdb.register("Poblacion", Poblacion)
duckdb.register("Departamento", Depto)
duckdb.register("Provincia", Provincia)  
duckdb.register("BP", BP_limpio)

consulta1 = """
WITH ee_por_departamento AS (
    SELECT 
        id_departamento,
        COUNT(DISTINCT CASE WHEN Jardin > 0 THEN Cueanexo END) AS jardines,
        COUNT(DISTINCT CASE WHEN Primario > 0 THEN Cueanexo END) AS primarias,
        COUNT(DISTINCT CASE WHEN Secundario > 0 THEN Cueanexo END) AS secundarios
    FROM EE
    GROUP BY id_departamento
),
poblacion_por_departamento AS (
    SELECT 
        id_departamento,
        SUM(CASE WHEN "Rango etario" = '3 a 5' THEN casos ELSE 0 END) AS poblacion_jardin,
        SUM(CASE WHEN "Rango etario" = '6 a 11' THEN casos ELSE 0 END) AS poblacion_primaria,
        SUM(CASE WHEN "Rango etario" = '12 a 18' THEN casos ELSE 0 END) AS poblacion_secundaria
    FROM Poblacion
    GROUP BY id_departamento
)

SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,

    COALESCE(ee.jardines, 0) AS jardines,
    COALESCE(pob.poblacion_jardin, 0) AS poblacion_jardin,

    COALESCE(ee.primarias, 0) AS primarias,
    COALESCE(pob.poblacion_primaria, 0) AS poblacion_primaria,

    COALESCE(ee.secundarios, 0) AS secundarios,
    COALESCE(pob.poblacion_secundaria, 0) AS poblacion_secundaria

FROM Depto dept
JOIN Provincia prov ON dept.id_provincia = prov.id
LEFT JOIN ee_por_departamento ee ON dept.id_departamento = ee.id_departamento
LEFT JOIN poblacion_por_departamento pob ON dept.id_departamento = pob.id_departamento

ORDER BY prov.nombre ASC, primarias DESC;
"""

resultado = duckdb.query(consulta1).to_df()
print(f"resultados 1: {resultado}")




consulta2 = """
SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,
    COUNT(*) AS bp_desde_1950

FROM BP bp
JOIN Departamento dept ON bp.id_departamento = dept.id_departamento
JOIN Provincia prov ON dept.id_provincia = prov.id

WHERE 
    TRY_CAST(SUBSTR(bp.fecha_fundacion, 1, 4) AS INTEGER) >= 1950

GROUP BY prov.nombre, dept.Departamento
ORDER BY prov.nombre ASC, bp_desde_1950 DESC;
"""

resultado2 = duckdb.query(consulta2).to_df()
#print(f"resultados 2: {resultado2}")


consulta3 = """
SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,

    COUNT(DISTINCT bp.nro_conabip) AS cantidad_bp,
    COUNT(DISTINCT ee.Cueanexo) AS cantidad_ee,
    SUM(pobl.casos) AS poblacion_total

FROM Departamento dept
JOIN Provincia prov ON dept.id_provincia = prov.id
LEFT JOIN BP bp ON bp.id_departamento = dept.id_departamento
LEFT JOIN EE ee ON ee.id_departamento = dept.id_departamento
LEFT JOIN Poblacion pobl ON pobl.id_departamento = dept.id_departamento

GROUP BY prov.nombre, dept.Departamento
ORDER BY cantidad_ee DESC, cantidad_bp DESC, prov.nombre ASC, dept.Departamento ASC;
"""

resultado3 = duckdb.query(consulta3).to_df()
#print(f"resultados 3: {resultado3}")


consulta4 = """
SELECT provincia, departamento, dominio AS dominio_mas_frecuente
FROM (
  SELECT 
    prov.nombre          AS provincia,
    dept.Departamento    AS departamento,
    SPLIT_PART(bp.mail, '@', 2)   AS dominio,
    COUNT(*)             AS cnt,
    ROW_NUMBER() OVER (
      PARTITION BY dept.id_departamento 
      ORDER BY COUNT(*) DESC
    )                     AS rn
  FROM BP bp
  JOIN Departamento dept 
    ON bp.id_departamento = dept.id_departamento
  JOIN Provincia prov 
    ON dept.id_provincia = prov.id
  WHERE bp.mail IS NOT NULL
    AND bp.mail LIKE '%@%'
  GROUP BY prov.nombre, dept.Departamento, dept.id_departamento, dominio
)
WHERE rn = 1
ORDER BY provincia ASC, departamento ASC;
"""

resultado4 = duckdb.query(consulta4).to_df()
#print(f"resultados 4: {resultado4}")

