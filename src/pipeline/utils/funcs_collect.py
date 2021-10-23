"""
COLLECT DATA
"""

import requests
import pandas as pd
import numpy as np
import os

from pandas import json_normalize
from pandas import DataFrame
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile


def get_datoscdmx(resource_id, *search_word, distinct_rows:bool=True, limit:int=32000, sort:str='id desc', plain:bool=False, query=False)-> DataFrame:
    """Get data from DATOS CDMX API"""

    url = 'https://datos.cdmx.gob.mx/api/3/action/datastore_search?resource_id='
    params = f'&distinct={"true" if distinct_rows else "false"}&limit={limit}&sort={sort}&plain={plain}'
    if query==True:
        params=params+f'&q={search_word[0]}'
    else:
        pass
    df = json_normalize(requests.get(url+resource_id+params).json()['result']['records'])
    return(df)

def unzip(http_response, extract_to):
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=extract_to)


def download(url, filename):
    http_response = urlopen(url)
    try:
        extract_to = create_folder(os.getcwd()+f'../../../data/raw/{filename}/')
    except:
        pass
    unzip(http_response, extract_to)


def create_folder(path):
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)
    return(path)
