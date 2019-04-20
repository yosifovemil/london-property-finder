from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import requests
import pandas as pd


def get_lsoa(longitude, latitude):
    """Coordinates to LSOA"""
    url = 'http://api.postcodes.io/postcodes?lon=%f&lat=%f&radius=1000' % (longitude, latitude)
    output = requests.get(url)
    if output.json()['result'] is not None:
        result = pd.DataFrame.from_records(output.json()['result'])
        return result.lsoa.value_counts().idxmax()
    else:
        return None


def get_outcode_from_address(locations):
    """Extract first 3-4 alphanumerics from postcode"""
    outcodes = []
    for location in locations:
        if location is not None:
            outcodes.append(location.address.split(',')[-2].strip().split(' ')[0])
        else:
            outcodes.append(None)

    return outcodes


def get_coordinates(data, geocoder):
    """
    Applies geocoder to dataset - returns only rows that were successfully processed by the geocoder
    :param data: data left to process
    :param geocoder: name of the geocoder to use
    :return:
    """

    # choose geocoder
    if geocoder == 'Nominatim':
        geolocator = Nominatim(user_agent="HouseFinder")
    else:
        raise Exception("Geocoder %s not implemented" % geocoder)

    # limit requests rate
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # apply geocoder
    data['Location'] = data.address.apply(geocode)

    # check if found addresses match what we requested
    data['Outcode2'] = get_outcode_from_address(data.Location)
    data = data[data.Outcode == data.Outcode2]

    # extract coordinates
    data['Latitude'] = data.Location.apply(lambda x: x.latitude)
    data['Longitude'] = data.Location.apply(lambda x: x.longitude)

    # cleanup columns
    data = data[['ID', 'price', 'type', 'url', 'number_bedrooms', 'Latitude', 'Longitude']]

    data = data.rename(columns={'price': 'Price',
                                'type': 'Type',
                                'url': 'URL',
                                'number_bedrooms': 'Bedrooms'})

    # get LSOA for each location
    data['LSOA'] = data.apply(lambda x: get_lsoa(x.Longitude, x.Latitude), axis=1)

    return data


def get_location_info(data):
    data = data.sample(30)

    frames = []
    for geocoder in ['Nominatim']:
        frames.append(get_coordinates(data, geocoder))
        output_data = pd.concat(frames)
        data = data[~data.ID.isin(output_data.ID)]
