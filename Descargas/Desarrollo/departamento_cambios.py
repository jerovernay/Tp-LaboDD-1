import pandas as pd

Departamentos = pd.read_csv(r"C:\Users\gasto\Downloads\Departamento.csv")

Departamentos['id_departamento'] = Departamentos['id_departamento'].replace({
    94015: 94014,
    94008: 94007,
    2007: 2101,
    2014: 2102,
    2021: 2103,
    2028: 2104,
    2035: 2105,
    2042: 2106,
    2049: 2107,
    2056: 2108,
    2063: 2109,
    2070: 2110,
    2077: 2111,
    2084: 2112,
    2091: 2113,
    2098: 2114,
})

Departamentos.loc[Departamentos['Departamento'] == 'Comuna 15', 'id_departamento'] = Departamentos.loc[Departamentos['Departamento'] == 'Comuna 15', 'id_departamento'].replace(2105, 2115)


Departamentos.to_csv('Departamento_corregido.csv', index=False)
