import requests
import pandas as pd
import time
from functools import partial
from property_finder.tfl_credentials import TFL_CREDENTIALS
import dask.dataframe as dd
from dask.diagnostics import ProgressBar



# TODO remove hardcoded parameters from URL
TFL_URL = 'https://api.tfl.gov.uk/Journey/JourneyResults/' \
          '%f%%2C%%20%f/to/%f%%2C%%20%f?nationalSearch=true&date=20190527&time=%s&timeIs=%s&app_id=%s&app_key=%s'


def get_single_journey(origin, destination, time_str, time_is):
    loaded = False
    while not loaded:
        try:
            result = requests.get(TFL_URL % (origin[0], origin[1],
                                             destination[0], destination[1],
                                             time_str, time_is, TFL_CREDENTIALS['app_id'],
                                             TFL_CREDENTIALS['app_key'])).json()

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


def process_row(destination, row):
    result = {'ID': None,
              'JourneyDuration': None,
              'JourneyFare': None,
              'Walking': None,
              'Train': None,
              'Underground': None,
              'Bus': None}

    origin = (row.Latitude, row.Longitude)

    journey = get_monthly_journey(origin, destination,
                                  '0845', '1715')
    for key in [x for x in result.keys() if x != 'ID']:
        result[key] = journey[key]

    result['ID'] = row.ID
    return result


def update_travel_info(database, config):
    location_data = database.read_table(table=config['database']['LocationTable'])

    destination = (float(config['journey']['destination_lat']), float(config['journey']['destination_lon']))
    get_journey = partial(process_row, destination)

    location_data = dd.from_pandas(location_data, chunksize=1)
    travel_data = location_data.apply(get_journey, axis=1, meta=(None, 'object'))

    with ProgressBar():
        travel_data = travel_data.compute()

    travel_data = pd.DataFrame.from_records([x for x in travel_data if x is not None])

    database.truncate_table(config['database']['TravelTable'])
    database.write_table(travel_data, config['database']['TravelTable'])
