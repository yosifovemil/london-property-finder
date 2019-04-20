import requests
import pandas as pd
import time


TFL_URL = 'https://api.tfl.gov.uk/Journey/JourneyResults/' \
          '%f%%2C%%20%f/to/%f%%2C%%20%f?nationalSearch=true&time=%s&timeIs=%s'


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
