import geopandas as gpd
import pandas as pd


def get_lon_lat(df, geo_point="geo_point_2d"):
    """GET LONGITUD AND LATITUD FROM GEOPOINT"""

    df = pd.concat([df, df[geo_point].\
                  str.split(",", n = 1, expand = True)], axis=1)
    df.rename(
        columns={0: 'longitud', 1: 'latitud'},
        inplace=1
    )
    df['longitud'] = df['longitud'].astype('float128')
    df['latitud'] = df['latitud'].astype('float128')
    return(df)


def convert_geopandas(df, x='latitud', y='longitud'):
    df = gpd.GeoDataFrame(
        df,
        geometry = gpd.points_from_xy(df[y], df[x], ))
    df = df.set_crs("EPSG:4326")
    return(df)

def get_lon_lat_geometry(df):
    """GET LONGITUD AND LATITUD FROM GEOPOINT"""

    df['centroid'] = df['geometry'].centroid
    #Extract lat and lon from the centerpoint
    df["latitud"] = df.centroid.map(lambda p: p.x)
    df["longitud"] = df.centroid.map(lambda p: p.y)
    return(df)
