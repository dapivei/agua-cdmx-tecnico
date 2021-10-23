"""
MERGE CON DATOS DE MANZANAS TOTALES EN CDMX E IMPUTACIÓN DE VALORES FALTANTES
"""

import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np

sys.path.insert(0, os.getcwd().split('src')[0])

import src
from src.utils import funcs_collect, funcs_geo, funcs_knn, funcs_result

## GET UNIQUE VALUES OF AGUA PER MANZANA

agua = pd.read_parquet('../../data/raw/datos-cdmx/agua_bimestres.parquet')
id_column='geo_point_2d'
indice_des_max = agua.groupby(

    [id_column]

).indice_des.max().reset_index(

)
colonia_alcaldia = agua[[id_column, 'colonia', 'alcaldia']].\
drop_duplicates(id_column)

unique_manzanas=indice_des_max.merge(
    colonia_alcaldia,
    on = id_column,
    how='left'
)
agua = agua.groupby(

    ['geo_point_2d']

).mean().reset_index()
unique_manzanas = unique_manzanas.merge(
    agua,
    on = id_column,
    how = 'left'
)
unique_manzanas = unique_manzanas.drop(
    ['gid', 'bimestre', '_id', 'id', 'anio'],
    axis=1
)
agua = funcs_geo.get_lon_lat(
    unique_manzanas
)
agua = funcs_geo.convert_geopandas(agua)
agua = agua.fillna(0)


# MANZANAS CDMX

manzanas = gpd.read_file('../../data/raw/manzanas/09_Manzanas_INV2016.shp')
manzanas.columns= manzanas.columns.str.strip().str.lower()
manzanas=manzanas.replace('N.D.', np.nan)
manzanas=manzanas.replace('*', np.nan)
manzanas=manzanas.replace('*********', np.nan)

## MERGE MANZANAS WITH AGUA CDMX

agua_manzanas = gpd.sjoin(

    agua,
    manzanas,
    how= "right",
    op='within'


).drop_duplicates(

    'geometry'

).reset_index(

    drop = True

)
agua_manzanas = agua_manzanas[[
       'cvegeo',
       'indice_des',
       'rank',
       'consumo_total_mixto',
       'consumo_prom_dom',
       'consumo_total_dom',
       'consumo_prom_mixto',
       'consumo_total',
       'consumo_prom',
       'consumo_prom_no_dom',
       'consumo_total_no_dom',
       'vivtot',
       'tvivhab',
       'p_tvivhab',
       'tvivpar',
       'p_tvivpar',
       'tvivparhab',
       'pvivparhab',
       'vivpar_des',
       'p_vivpar_d',
       'vivpar_ut',
       'p_vivpar_u',
       'vivnohab',
       'p_vivnohab',
       'vph_pisodt',
       'p_v_pisodt',
       'vph_c_elec',
       'p_v_c_elec',
       'vph_aguadv',
       'p_v_aguadv',
       'vph_drenaj',
       'p_v_drenaj',
       'vph_excusa',
       'p_v_excusa',
       'v_3masocup',
       'p_3masocup',
       'proocup_c',
       'pobtot',
       'p0a14a',
       'pp0a14a',
       'p15a29a',
       'pp15a29a',
       'p30a59a',
       'pp30a59a',
       'p_60ymas',
       'pp_60ymas',
       'pcon_lim',
       'ppcon_lim',
       'graproes',
       'acesoper_',
       'acesoaut_',
       'recucall_',
       'senaliza_',
       'alumpub_',
       'telpub_',
       'banqueta_',
       'guarnici_',
       'arboles_',
       'rampas_',
       'puessemi_',
       'puesambu_',
       'acesoper_c',
       'acesoaut_c',
       'recucall_c',
       'senaliza_c',
       'alumpub_c',
       'telpub_c',
       'banqueta_c',
       'guarnici_c',
       'arboles_c',
       'rampas_c',
       'puessemi_c',
       'puesambu_c',
       'ent',
       'nom_ent',
       'mun',
       'nom_mun',
       'loc',
       'nom_loc',
       'ageb',
       'mza',
       'fecha_poli',
       'fecha_inf',
       'fecha_ceu',
       'geometry'
]]

agua_manzanas = funcs_geo.get_lon_lat_geometry(agua_manzanas)

# FANCYIMPUTER IMPUTATION
agua_manzanas_fancy = funcs_knn.fancy_imputer(agua_manzanas)
agua_manzanas_fancy = funcs_result.get_imputed_data(agua_manzanas_fancy)
agua_manzanas_fancy.to_file('../../data/transform/manzanas_consumo_fancyimput.shp')

# SKLEARN KNN IMPUTATION

agua_manzanas_sklearn = funcs_knn.sklearn_imputer(agua_manzanas)
agua_manzanas_sklearn = funcs_result.get_imputed_data(agua_manzanas_sklearn)
agua_manzanas_sklearn.to_file('../../data/transform/manzanas_consumo_sklearnimput.shp')
