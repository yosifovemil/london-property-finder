from property_finder.utility.url import get_url
from dask.diagnostics import ProgressBar

from rightmove_webscraper import rightmove_data
import pandas as pd
import dask.dataframe as dd
from functools import partial


def _get_rightmove_codes(rightmove_codes_file, search_outcodes_file):
    f = open(rightmove_codes_file)
    data = f.readlines()[0]
    f.close()

    data = pd.DataFrame(eval(data))
    data.rename(inplace=True, columns={'outcode': 'Outcode', 'code': 'Code'})

    outcodes = _get_outcodes(search_outcodes_file)
    data = data.merge(outcodes, on='Outcode')
    return data


def _get_outcodes(search_outcodes_file):
    return pd.read_csv(search_outcodes_file)


def _scrape_outcode(url, entry):
    try:
        scrape = rightmove_data(url % entry.Code)
        results = scrape.get_results
        results['Outcode'] = entry.Outcode
        return results

    except ValueError as err:
        content = get_url(url % entry.Code)
        if "We couldn't find what you’re looking for right now" in content:
            # error raised because no places were available - expected behaviour
            pass
        else:
            raise err


def _extract_rightmove_id(urls):
    urls = urls.str.replace(r'.*\/property-', '')
    urls = urls.str.replace(r'\.html', '')
    urls = urls.astype(int)
    return urls


def _scrape_rightmove(config):
    rightmove_codes = _get_rightmove_codes(config['rightmove']['rightmove_codes'],
                                           config['rightmove']['search_outcodes'])

    rightmove_codes = dd.from_pandas(rightmove_codes, chunksize=1)

    outcode_partial = partial(_scrape_outcode, config['rightmove']['URL'])
    data = rightmove_codes.apply(outcode_partial, axis=1, meta=(None, 'object'))
    with ProgressBar():
        data = data.compute()

    columns = ['price', 'type', 'address', 'url', 'agent_url', 'postcode',
               'number_bedrooms', 'search_date', 'Outcode']
    data = pd.concat([pd.DataFrame(x, columns=columns) for x in data.values if x is not None])

    data.rename(columns={'price': 'Price',
                         'type': 'Type',
                         'url': 'URL',
                         'number_bedrooms': 'Bedrooms'}, inplace=True)

    data['ID'] = _extract_rightmove_id(data.URL)
    data = data[['ID', 'Price', 'Type', 'URL', 'Bedrooms']]
    data = data.drop_duplicates()

    return data


def update_rightmove(database, config):
    data = _scrape_rightmove(config)
    database.truncate_table(config['database']['PropertyTable'])
    database.write_table(data, config['database']['PropertyTable'])
