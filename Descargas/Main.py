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


# Generamos las tablas del modelo relacional 
consulta_provincia = """
                        SELECT DISTINCT SUBSTR(id_departamento, 1, 2) AS id, Jurisdicción AS nombre
                        FROM establecimientos_ed
                    """
provincia = duckdb.query(consulta_provincia).df()

consulta_departamento = """
                        SELECT DISTINCT id_departamento, departamento, SUBSTR(id_departamento, 1, 2) AS id_provincia
                        FROM establecimientos_ed
                    """
departamento = duckdb.query(consulta_departamento).df()