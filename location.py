from geopy.geocoders import Nominatim
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm


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
        geolocator = Nominatim(user_agent="LondonPropertyFinder",
                               domain='localhost/nominatim',
                               country_bias='United Kingdom',
                               scheme='http',
                               view_box=[(51.201721, -0.799255), (51.828988, 0.477905)],
                               timeout=10)
    else:
        raise Exception("Geocoder %s not implemented" % geocoder)

    # apply geocoder
    start = datetime.now()
    tqdm.pandas()
    print("Running %s" % geocoder)
    data['Location'] = data.address.progress_apply(lambda x: geolocator.geocode(x, bounded=True))
    print(datetime.now() - start)

    # check if found addresses match what we requested
    data['Outcode2'] = get_outcode_from_address(data.Location)
    data = pd.DataFrame(data[data.Outcode == data.Outcode2])

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
    data['LSOA'] = data.progress_apply(lambda x: get_lsoa(x.Longitude, x.Latitude), axis=1)

    return data


def get_location_info(data):
    frames = []
    for geocoder in ['Nominatim']:
        frames.append(get_coordinates(data, geocoder))
        output_data = pd.concat(frames)
        data = data[~data.ID.isin(output_data.ID)]

    return pd.concat(frames)
