import duckdb
import pandas as pd

BP_limpio = pd.read_csv("BP_limpio.csv")
EE_limpio01 = pd.read_csv("EE_limpio_final_usando2daopcion.csv")
Provincia = pd.read_csv("Provincia.csv")
Depto = pd.read_csv("Departamento_corregido.csv")
Poblacion = pd.read_csv("Padron_limpio_final.csv")


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
#print(f"resultados 1: {resultado}")




consulta2 = """
SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,
    COUNT(*) AS Cant_BP_fundadas_desde_1950

FROM BP bp
JOIN Departamento dept ON bp.id_departamento = dept.id_departamento
JOIN Provincia prov ON dept.id_provincia = prov.id

WHERE 
    TRY_CAST(SUBSTR(bp.fecha_fundacion, 1, 4) AS INTEGER) >= 1950

GROUP BY prov.nombre, dept.Departamento
ORDER BY prov.nombre ASC, Cant_BP_fundadas_desde_1950 DESC;
"""

resultado2 = duckdb.query(consulta2).to_df()
#print(f"resultados 2: {resultado2}")



consulta3 = """
WITH bp_por_dpto AS (
    SELECT 
        id_departamento,
        COUNT(DISTINCT nro_conabip) AS cantidad_bp
    FROM BP
    GROUP BY id_departamento
),
ee_por_dpto AS (
    SELECT 
        id_departamento,
        COUNT(DISTINCT Cueanexo) AS cantidad_ee
    FROM EE
    GROUP BY id_departamento
),
poblacion_por_dpto AS (
    SELECT 
        id_departamento,
        SUM(casos) AS poblacion_total
    FROM Poblacion
    GROUP BY id_departamento
)

SELECT 
    prov.nombre AS provincia,
    dept.Departamento AS departamento,
    COALESCE(bp.cantidad_bp, 0) AS cantidad_bp,
    COALESCE(ee.cantidad_ee, 0) AS cantidad_ee,
    COALESCE(pob.poblacion_total, 0) AS poblacion_total

FROM Departamento dept
JOIN Provincia prov ON dept.id_provincia = prov.id
LEFT JOIN bp_por_dpto bp ON dept.id_departamento = bp.id_departamento
LEFT JOIN ee_por_dpto ee ON dept.id_departamento = ee.id_departamento
LEFT JOIN poblacion_por_dpto pob ON dept.id_departamento = pob.id_departamento

ORDER BY cantidad_ee DESC, cantidad_bp DESC, prov.nombre ASC, dept.Departamento ASC;
"""

resultado3 = duckdb.query(consulta3).to_df()
#print(f"resultados 3: {resultado3}")


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
      SPLIT_PART(bp.mail, '@', 2) AS dominio,   /* Agarro la parte después del @ */
      COUNT(*) AS count,    /* Cuenta repeticiones de cada dominio */
      ROW_NUMBER() OVER (
        PARTITION BY dept.id_departamento  /* Agrupo por departamento */
        ORDER BY COUNT(*) DESC  /* Ordeno dominios de mayor a menor */
      ) AS nro_fila /* la primer fila sera la mas frecuente */
    FROM BP bp
    JOIN Departamento dept ON bp.id_departamento = dept.id_departamento
    JOIN Provincia prov ON dept.id_provincia = prov.id
    WHERE bp.mail IS NOT NULL AND bp.mail LIKE '%@%'    /* Filtro mails válidos */
    GROUP BY prov.nombre, dept.Departamento, dept.id_departamento, dominio
  ) t
  WHERE nro_fila = 1
) final_result
ORDER BY provincia ASC, departamento ASC;
"""

resultado4 = duckdb.query(consulta4).to_df()
print(f"resultados 4: {resultado4}")
resultado4.to_csv("Consulta_4.csv", index = False)
