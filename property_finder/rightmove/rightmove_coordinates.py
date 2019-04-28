from property_finder.utility import get_url
import re
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd


CLEANUP_LOCATIONS = """
DELETE FROM %s WHERE
ID NOT IN (SELECT ID FROM %s)
"""


def _extract_coordinate(name, string):
    coord_string = _apply_pattern('%s=.*?&' % name, string)
    return float(coord_string.replace('&', '').split('=')[1])


def _apply_pattern(pattern, string):
    p = re.compile(pattern)
    result = p.search(string)
    return result.group(0)


def _get_property_coordinates(property):
    if property.URL == 'foo':
        return

    content = get_url(property.URL, 'iso-8859-1')

    # get img element with the map
    map_element = _apply_pattern('<img src="//media.rightmove.co.uk/map/_generate.*Get map and local information"/>',
                                 content)

    # extract longitude from element
    latitude = _extract_coordinate('latitude', map_element)
    longitude = _extract_coordinate('longitude', map_element)

    return property.ID, latitude, longitude


def update_coordinates(database, config):
    properties = database.read_table(config['database']['PropertyTable'])
    locations = database.read_table(config['database']['LocationTable'])

    # get coordinates for properties we don't have information for
    new_properties = pd.DataFrame(properties[~properties.ID.isin(locations.ID)])

    if len(new_properties) > 0:
        # lookup coordinate of each property in parallel using dask
        new_properties = dd.from_pandas(new_properties, chunksize=1)
        new_locations = new_properties.apply(_get_property_coordinates, axis=1, meta=(None, 'object'))
        with ProgressBar():
            new_locations = new_locations.compute()

        new_locations = pd.DataFrame({'ID': [x[0] for x in new_locations],
                                      'Latitude': [x[1] for x in new_locations],
                                      'Longitude': [x[2] for x in new_locations]})

        database.write_table(new_locations, config['database']['LocationTable'])

    # remove location we no longer need
    database.run_sql(CLEANUP_LOCATIONS % (config['database']['LocationTable'],
                                          config['database']['PropertyTable']))
