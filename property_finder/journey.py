import requests
import pandas as pd
import time
from functools import partial
from property_finder.tfl_credentials import TFL_CREDENTIALS
from ratelimit import limits, sleep_and_retry
from tqdm import tqdm
from datetime import datetime


# TODO remove hardcoded parameters from URL
TFL_URL = 'https://api.tfl.gov.uk/Journey/JourneyResults/' \
          '%f%%2C%%20%f/to/%f%%2C%%20%f?nationalSearch=true&date=20190527&time=%s&timeIs=%s&app_id=%s&app_key=%s'


@sleep_and_retry
@limits(calls=500, period=60)
def get_single_journey(origin, destination, time_str, time_is):
    loaded = False
    retries = 0

    while not loaded:
        try:
            result = requests.get(TFL_URL % (origin[0], origin[1],
                                             destination[0], destination[1],
                                             time_str, time_is, TFL_CREDENTIALS['app_id'],
                                             TFL_CREDENTIALS['app_key'])).json()

            journey = result['journeys'][0]
            loaded = True
        except:
            print("[%s] Could not connect to TFL API" % datetime.now())
            retries += 1
            if retries == 10:
                raise Exception("Could not match journey")

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


def process_row(destination, database, config, row):
    result = {'ID': [],
              'JourneyDuration': [],
              'JourneyFare': [],
              'Walking': [],
              'Train': [],
              'Underground': [],
              'Bus': []}

    origin = (row.Latitude, row.Longitude)

    try:
        journey = get_monthly_journey(origin, destination,
                                      '0845', '1715')
        for key in [x for x in result.keys() if x != 'ID']:
            result[key].append(journey[key])

        result['ID'].append(row.ID)

        result = pd.DataFrame(result)
        database.write_table(result, config['database']['TravelTable'])
    except Exception as e:
        print('ID: %s %s' % (row.ID, str(e)))


def update_travel_info(database, config):
    location_data = database.read_table(table=config['database']['LocationTable'])
    travel_data = database.read_table(table=config['database']['TravelTable'])

    location_data = location_data[~location_data.ID.isin(travel_data.ID)]

    destination = (float(config['journey']['destination_lat']), float(config['journey']['destination_lon']))
    get_journey = partial(process_row, destination, database, config)

    tqdm.pandas()
    location_data.progress_apply(get_journey, axis=1)
