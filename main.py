import pandas as pd
import googlemaps
from datetime import datetime
from rightmove import get_rightmove
from location import get_location_info
from journey import attach_travel_info
import time
import os


pd.set_option('display.width', 200)
pd.set_option('max.columns', 200)


def multiple_deprivation():
    """Read multiple deprivation index file"""
    data = pd.read_excel('data/multiple_deprivation.xlsx')
    data = data[[data.columns[1], data.columns[4]]]
    data.columns = ['LSOA', 'MultipleDeprivationIndex']
    return data


def journey_time(latitude, longitude):
    API_KEY = 'AIzaSyAAhb3nc2HwJnrEQzkm3M1N2zVfZdUwsu0'
    gmaps = googlemaps.Client(key=API_KEY)

    # Request directions via public transit
    arr_time = datetime(2019, 4, 8, 9, 0)
    directions_result = gmaps.directions("%f,%f" % (latitude, longitude),
                                         "Farringdon Underground Station",
                                         mode="transit",
                                         arrival_time=arr_time)
    duration = directions_result[0]['legs'][0]['duration']
    return duration['value']


def attach_location_info(data):
    cache_file = 'cache/location_info.pickle'
    new_columns = {'ID': [],
                   'Latitude': [],
                   'Longitude': [],
                   'LSOA': []}

    if os.path.exists(cache_file):
        cache_data = pd.read_pickle(cache_file)
        for key in new_columns.keys():
            new_columns[key] = list(cache_data[key])

    iter_data = data.copy()
    iter_data = iter_data[~iter_data.ID.isin(new_columns['ID'])]

    for row in iter_data.iterrows():
        passed = False
        while not passed:
            try:
                latitude, longitude, lsoa = get_location_info(row[1].address)
                new_columns['ID'].append(row[1].ID)
                new_columns['Latitude'].append(latitude)
                new_columns['Longitude'].append(longitude)
                new_columns['LSOA'].append(lsoa)
                pd.DataFrame(new_columns).to_pickle('cache/location_info.pickle')
                time.sleep(1.0)
                passed = True
            except Exception as e:
                print(str(e))
                print("Location call failed waiting for 10 minutes")
                time.sleep(600)

    data = data.merge(pd.DataFrame(new_columns), on='ID')

    return data


if __name__ == '__main__':
    start = datetime.now()

    stage_file = 'cache/stage1.pickle'
    if os.path.exists(stage_file):
        data = pd.read_pickle(stage_file)
    else:
        print('Scraping Rightmove')
        data = get_rightmove()
        data['ID'] = range(len(data))
        data.to_pickle(stage_file)
        print(datetime.now() - start)

    stage_file = 'cache/stage2.pickle'
    if os.path.exists(stage_file):
        data = pd.read_pickle(stage_file)
    else:
        print('Getting location info')
        data = get_location_info(data)
        data.to_pickle(stage_file)
        print(datetime.now() - start)

    stage_file = 'cache/stage3.pickle'
    if os.path.exists(stage_file):
        data = pd.read_pickle(stage_file)
    else:
        print('Attaching multiple deprivation index')
        bad_data = data[data.LSOA.isnull()]
        data = data[~data.LSOA.isnull()]
        md = multiple_deprivation()
        data = data.merge(md, on='LSOA')
        data.to_pickle(stage_file)
        print(datetime.now() - start)

    stage_file = 'cache/stage4.pickle'
    if os.path.exists(stage_file):
        data = pd.read_pickle(stage_file)
    else:
        print('Attaching travel info')
        data = attach_travel_info(data)
        data.to_pickle(stage_file)
        print(datetime.now() - start)
