"""
KNN IMPUTERS FOR TREATMENT OF MISSING DATA
"""
import pandas as pd
import sklearn
import fancyimpute

from sklearn.preprocessing import OrdinalEncoder
from fancyimpute import KNN
from sklearn.impute import KNNImputer

def encode(data):
    """
    Function to encode non null data and replace
    it in the original data
    """
    nonulls = np.array(data.dropna())
    impute_reshape = nonulls.reshape(-1,1)
    impute_ordinal = encoder.fit_transform(impute_reshape)
    data.loc[data.notnull()] = np.squeeze(impute_ordinal)
    return data

def fancy_imputer(df):

    """Knn imputer"""

    df = df[[
    'indice_des',
    'consumo_total',
    'consumo_prom',
    'vivtot',
    'latitud',
    'longitud',
    'nom_mun',
    'nom_loc',
    'pobtot',
    'vph_drenaj'
    ]]
    encoder = OrdinalEncoder()
    imputer = KNN()
    cat_cols = [
            'indice_des',
            'nom_mun',
            'nom_loc'
           ]
    for columns in cat_cols:
    encode(df[columns])
    df = pd.DataFrame(
    imputer.fit_transform(df),
    columns = df.columns
    )
    df=df.drop(['nom_loc', 'nom_mun'],axis=1)
    df=df.round({'indice_des': 0})
    df["indice_des"].replace(
    {1.0: "BAJO", 0.0: "ALTO", 2.0:"MEDIO", 3.0:"POPULAR"},
    inplace=True
    )
    return(df)

def sklearn_imputer(df):

    """Knn imputer"""

    df = df[[
    'indice_des',
    'consumo_total_mixto',
    'consumo_prom_dom',
    'consumo_total_dom',
    'consumo_prom_mixto',
    'consumo_total',
    'consumo_prom',
    'consumo_prom_no_dom',
    'consumo_total_no_dom',
    'vivtot',
    'latitud',
    'longitud',
    'nom_mun',
    'nom_loc',
    'pobtot',
    'vph_drenaj'
    ]]
    cat_variables = df[[
                'indice_des',
                'nom_mun',
                'nom_loc'
               ]]
    cat_dummies = pd.get_dummies(cat_variables)
    df = df.drop([
                'indice_des',
                'nom_mun',
                'nom_loc']
               , axis=1)
    df = pd.concat([df, cat_dummies], axis=1)

    imputer = KNNImputer(n_neighbors=5)
    df = pd.DataFrame(
        imputer.fit_transform(df),
        columns = df.columns
    )
    df = df[[
    'consumo_total_mixto',
    'consumo_prom_dom',
    'consumo_total_dom',
    'consumo_prom_mixto',
    'consumo_total',
    'consumo_prom',
    'consumo_prom_no_dom',
    'consumo_total_no_dom',
    'vivtot',
    'latitud',
    'longitud',
    'pobtot',
    'vph_drenaj',
    'indice_des_ALTO',
    'indice_des_BAJO',
    'indice_des_MEDIO',
    'indice_des_POPULAR'
    ]]
    return(df)
