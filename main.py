from property_finder.rightmove.rightmove_properties import update_rightmove
from property_finder.rightmove.rightmove_coordinates import update_coordinates
from property_finder.database import Database

import json


def stage_required(config, stage):
    res = config['run_stages'][stage]
    return eval(res)


def main():
    # read config file
    with open('config.json') as json_data_file:
        config = json.load(json_data_file)

    # set up the database
    database = Database(config['database']['path'])

    if stage_required(config, 'rightmove'):
        # scrape new rightmove data
        print('Scraping Rightmove')
        update_rightmove(database=database,
                         config=config)

    if stage_required(config, 'location'):
        # find coordinates for new properties
        print("Scraping coordinates")
        update_coordinates(database=database,
                           config=config)


if __name__ == '__main__':
    main()
