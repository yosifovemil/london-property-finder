import requests
import pandas as pd
import time
import os
from haversine import Haversine
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from tqdm import tqdm


# TODO remove hardcoded parameters from URL
TFL_URL = 'https://api.tfl.gov.uk/Journey/JourneyResults/' \
          '%f%%2C%%20%f/to/%f%%2C%%20%f?nationalSearch=true&date=20190429&time=%s&timeIs=%s'
DESTINATION = (51.520136, -0.104748)


def get_single_journey(origin, destination, time_str, time_is):
    loaded = False
    while not loaded:
        try:
            result = requests.get(TFL_URL % (origin[0], origin[1],
                                             destination[0], destination[1],
                                             time_str, time_is)).json()

            journey = result['journeys'][0]
            loaded = True
        except:
            time.sleep(0.2)

    output = dict()
    output['JourneyDuration'] = journey['duration']
    if 'fare' in journey.keys():
        output['JourneyFare'] = journey['fare']['totalCost']
    else:
        output['JourneyFare'] = None

    output['Walking'] = 0
    output['Train'] = 0
    output['Underground'] = 0
    output['Bus'] = 0
    for leg in journey['legs']:
        leg_mode = leg['mode']['name']
        if leg_mode == 'walking':
            mode = 'Walking'
        elif leg_mode == 'national-rail':
            mode = 'Train'
        elif leg_mode == 'tube':
            mode = 'Underground'
        elif leg_mode == 'bus':
            mode = 'Bus'

        output[mode] += leg['duration']

    return output


def get_monthly_journey(origin, destination, outbound_arrival, inbound_departure):
    journey1 = get_single_journey(origin, destination, time_str=outbound_arrival, time_is='Arriving')
    journey2 = get_single_journey(destination, origin, time_str=inbound_departure, time_is='Departing')
    result = pd.DataFrame([journey1, journey2]).sum()

    # average monthly working days
    result.JourneyFare = (result.JourneyFare/100.0) * 22

    return result


def process_row(row):
    result = {'ID': None,
              'JourneyDuration': None,
              'JourneyFare': None,
              'Walking': None,
              'Train': None,
              'Underground': None,
              'Bus': None}

    origin = (row.Latitude, row.Longitude)

    dist = Haversine(origin, DESTINATION)
    if dist.miles > 30.0:
        return None

    journey = get_monthly_journey(origin, DESTINATION,
                                  '0845', '1715')
    for key in [x for x in result.keys() if x != 'ID']:
        result[key] = journey[key]

    result['ID'] = row.ID
    return result


def attach_travel_info(data):
    new_columns = {'ID': [],
                   'JourneyDuration': [],
                   'JourneyFare': [],
                   'Walking': [],
                   'Train': [],
                   'Underground': [],
                   'Bus': []}

    cache_file = 'cache/travel_info.pickle'

    if os.path.exists(cache_file):
        cache_data = pd.read_pickle(cache_file)
        for key in new_columns.keys():
            new_columns[key] = list(cache_data[key].values)

    iter_data = data[~data.ID.isin(new_columns['ID'])]

    # iter_data = dd.from_pandas(iter_data, chunksize=100)

    # with ProgressBar():
    #     result = iter_data.apply(process_row, axis=1).compute()
    #     result = pd.DataFrame.from_records(result.values)
    tqdm.pandas()
    result = iter_data.progress_apply(process_row, axis=1)
    result = pd.DataFrame.from_records([x for x in result if x is not None])

    data = data.merge(result, on='ID', how='left')

    return data
