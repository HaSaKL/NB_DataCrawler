import argparse
import configparser

from NB_lib import NBMasterDataDB


class NBCLI:
    """"Class which defines the interface to the CL, using configuration files and command line arguments"""

    def _parse_cl(self):
        """"Defines and Parses the command line options an returns the corresponding namespace"""
        parser = argparse.ArgumentParser(description="Get Data from NextBike Servers and Save it to a Database.")

        parser.add_argument("-p", "--places", type=str, default="places.ini", help="ini-File with places")
        parser.add_argument("-d", "--database", type=str, default="database.ini", help="ini-File with data base info")
        parser.add_argument("-l", "--logfile", type=str, default="db_log.log", help="file name for logging")

        self.cmdl_args = parser.parse_args()

    def _parse_database_config(self):
        """"Takes the database.ini of a config file and stores the database info in the args"""

        # make a a parser and open the config file
        config = configparser.ConfigParser()
        try:
            assert self.cmdl_args.database.endswith('.ini')
            assert config.read(self.cmdl_args.database)
        except AssertionError:
            raise AssertionError("Wrong type of Database Configuration file. File should exist and end with .ini")

        # get the database info from the file
        if config.has_option("login", "file"):
            self.login_db_file = config.get("login", "file")
        else:
            self.login_db_file = "def_login.db"

        if config.has_option("stations_master", "file"):
            self.stations_master_db_file = config.get("stations_master", "file")
        else:
            self.stations_master_db_file = "def_stations_master.db"

        if config.has_option("station_transactions", "file"):
            self.stations_transactions_db_file = config.get("station_transactions", "file")
        else:
            self.stations_transactions_db_file = "def_stations_transactions.db"

    def _parse_place_config(self):
        """"Takes the location/name.ini of a config file and returns a list of uids of all places mentioned, no matter
        if they are in domains, cities or single places in the file"""

        # make parser and open places config file
        config = configparser.ConfigParser()
        try:
            assert self.cmdl_args.places.endswith('.ini')
            assert config.read(self.cmdl_args.places)
        except AssertionError:
            raise AssertionError("Wrong type of Place Configuration file. File should exist and end with .ini")

        # get a list of all stations
        for section in config.sections():

            if section == "domain":
                for domain in config["domain"].values():
                    if self.master_data.check_domain(domain):
                        self.places_list.extend(self.master_data.get_places_from_domain(domain))

            elif section == "city_uid":
                for city in config["city_uid"].values():
                    if self.master_data.check_city(city):
                        self.places_list.extend(self.master_data.get_places_from_city(city))

            elif section == "place_uid":
                for place in config["place_uid"].values():
                    if self.master_data.check_place(place):
                        self.places_list.append(place)

        # make place list unique
        self.places_list = list(set(self.places_list))

    def __init__(self):
        """" Parses the command line and the config files, if they are provided, will set up all important Information
        as instance variable """
        self.cmdl_args = None
        self._parse_cl()
        self._parse_database_config()
        self.master_data = NBMasterDataDB.NBMasterDataDB(master_data_db_name=self.stations_master_db_file,
                                                         login_data_db_name=self.login_db_file,
                                                         log_file=self.cmdl_args.logfile)
        # master data base needs to be filled in order to resolve the places list
        self.master_data.fill_if_empty()
        self.places_list = list()
        self._parse_place_config()
