import requests
import pandas as pd
import numpy as np
from tqdm import tqdm


def get_lsoa(longitude, latitude):
    """Coordinates to LSOA"""

    if longitude is None or latitude is None or np.isnan(longitude) or np.isnan(latitude):
        return None

    url = 'http://api.postcodes.io/postcodes?lon=%f&lat=%f&radius=1000' % (longitude, latitude)
    output = requests.get(url)

    if output.json()['result'] is not None:
        result = pd.DataFrame.from_records(output.json()['result'])
        return result.lsoa.value_counts().idxmax()
    else:
        return None


def multiple_deprivation():
    """Read multiple deprivation index file"""
    # TODO tidy up
    data = pd.read_excel('data/multiple_deprivation.xlsx')
    data = data[[data.columns[1], data.columns[4]]]
    data.columns = ['LSOA', 'MultipleDeprivationIndex']
    return data


def update_LSOA(database, config):
    tqdm.pandas()
    data = database.read_table(table=config['database']['LocationTable'])
    data['LSOA'] = data.progress_apply(lambda x: get_lsoa(x.Longitude, x.Latitude), axis=1)
    data = data[['ID', 'LSOA']]

    deprivation = multiple_deprivation()
    data = data.merge(deprivation, on='LSOA')

    database.write_table(data=data, table=config['database']['LSOATable'])
