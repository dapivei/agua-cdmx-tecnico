"""
RELEVANT DATA AND INDICATORS
"""

import pandas as pd

def get_imputed_data(df1, df2):
    imputed_data = pd.concat([df1, df2[['nom_loc', 'nom_mun']]], axis=1)
    imputed_data = pd.concat([df2[['cvegeo', 'geometry']], imputed_data], axis=1)
    imputed_data.columns = imputed_data.columns.str.strip().str.lower()

def indicator_1(df):
    """
    Estimar consumo promedio diario per capita, nivel delegación.
    """
    m = pd.DataFrame(((df.groupby('nom_mun').sum()['consumo_total']/df.groupby('nom_mun').sum()['pobtot'])*1000)/60, columns=['comsumo_diario_promedio_total_per_capita']).reset_index()
    m['superavit_deficit'] = round(m['comsumo_diario_promedio_total_per_capita']-100, 2)
    pob_total = pd.DataFrame(df.groupby('nom_mun').sum()['pobtot']).reset_index()
    viv_total = pd.DataFrame(df.groupby('nom_mun').sum()['vivtot']).reset_index()
    m['poblacion_total'] = round(pob_total.iloc[: , -1])
    m['viviendas_total'] = round(viv_total.iloc[: , -1])
    m = m.round({'comsumo_diario_promedio_total_per_capita': 2})
    return(m)
