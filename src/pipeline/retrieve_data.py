import sys
import os
sys.path.insert(0, os.getcwd().split('src')[0])
import src
import pandas as pd

from src.utils.funcs_collect import *

# Download MANZANAS shapefiles
URL_MANZANAS_CDMX = 'https://www.inegi.org.mx/contenidos/masiva/indicadores/inv/09_Manzanas_INV2016_shp.zip'
download(URL_MANZANAS_CDMX, 'manzanas')


# Download CONSUMO DE AGUA form API
df = list()
for nivel in ['BAJO', 'POPULAR', 'MEDIO', 'ALTO']:

    df.append(
    get_datoscdmx('2263bf74-c0ed-4e7c-bb9c-73f0624ac1a9', nivel, query=True)
    )

df = pd.concat(df).reset_index(drop=True)
df.to_parquet('../../data/raw/datos-cdmx/agua_bimestres.parquet')
