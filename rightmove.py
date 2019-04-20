from rightmove_webscraper import rightmove_data
import pandas as pd


URL = 'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=OUTCODE%%5E%d' \
      '&maxBedrooms=1&minBedrooms=1&maxPrice=1200&minPrice=800'
RIGHTMOVE_CODES = 'data/rightmove_outcodes.txt'
SEARCH_OUTCODES = 'data/search_outcodes.csv'


def _get_rightmove_codes():
    f = open(RIGHTMOVE_CODES)
    data = f.readlines()[0]
    f.close()

    data = pd.DataFrame(eval(data))
    data.rename(inplace=True, columns={'outcode': 'Outcode', 'code': 'Code'})

    outcodes = _get_outcodes()
    data = data.merge(outcodes, on='Outcode')
    return data


def _get_outcodes():
    return pd.read_csv(SEARCH_OUTCODES)


def _scrape_outcode(entry):
    try:
        scrape = rightmove_data(URL % entry.Code)
        results = scrape.get_results
        results['Outcode'] = entry.Outcode
        return results
    except:
        pass


def get_rightmove(n=None):
    rightmove_codes = _get_rightmove_codes()

    if n is not None:
        rightmove_codes = rightmove_codes.iloc[:n, ]

    data = rightmove_codes.apply(_scrape_outcode, axis=1)
    data = pd.concat(data.values)
    return data
