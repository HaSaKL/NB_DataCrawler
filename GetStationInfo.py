import argparse
import configparser
import sys
import NBMasterDataDB


def parse_place_config(config_file, master_db):
    """"Takes the location/name.ini of a config file and reads uid of the places mentioned in the file"""
    # make parser and open places config file
    config = configparser.ConfigParser()
    try:
        assert config_file.endswith('.ini')
        assert config.read(config_file)
    except AssertionError:
        sys.exit("Wrong Configuration File. File should exist and end with .ini")

    # get a list of all stations
    places_list = list()
    for section in config.sections():

        if section == "domain":
            for domain in config["domain"].values():
                if master_db.check_domain(domain):
                    places_list.extend(master_db.get_places_from_domain(domain))

        elif section == "city_uid":
            for city in config["city_uid"].values():
                if master_db.check_city(city):
                    places_list.extend(master_db.get_places_from_city())

        elif section == "place_uid":
            for place in config["place_uid"].values():
                if master_db.check_place(place):
                    places_list.append(place)

    # return list with unique places
    return list(set(places_list))


def ui_cli():
    """"Parses the command line options an returns the corresponding namespace"""
    parser = argparse.ArgumentParser(description="Get Data from NextBike Servers and Save it to a Database.")
    parser.add_argument("-p", "--places", type=str, default="places.ini", help="an ini-File with places")
    args_res = parser.parse_args()
    return args_res


if __name__ == '__main__':
    args = ui_cli()
    master_db = NBMasterDataDB.NBMasterDataDB()
    place_list = parse_place_config(args.places, master_db)
    print(place_list)
    print(len(place_list))
