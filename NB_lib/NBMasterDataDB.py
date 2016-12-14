import datetime
import logging
import sqlite3
import urllib.request
import xml.etree.ElementTree as ElmTree

from NB_lib import NBLoginDB


class NBMasterDataDB:
    """Class which defines an abstract interface to the master data base"""

    def __init__(self, master_data_db_name="stations_master.db",
                 login_data_db_name="login.db",
                 log_file="master_data.log"):
        """"Creates a database connection at initialization and establishes base DB-structure if necessary,
        also creates an NBLoginDB Object"""
        self.status_xml = None
        self.status_time = None

        # see if a logfile was set or if logging was disabled, if so, set logging flag to false or configure logging
        if not log_file:
            self.logging = False
        else:
            self.logging = True
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)s:%(message)s',
                                handlers=[logging.FileHandler(log_file, 'a', 'utf-8')],
                                datefmt="%Y-%m-%d %H.%M")

        self.login_db = NBLoginDB.NBLoginDB(databasename=login_data_db_name)
        self.conn = sqlite3.connect(master_data_db_name)

        c = self.conn.cursor()
        # see if database contains five tables and construct them if required
        # noinspection SqlResolve
        c.execute("SELECT `tbl_name` FROM sqlite_master WHERE type = 'table'")
        if len(c.fetchall()) < 5:
            c.execute("CREATE TABLE `places_data` (`uid` INTEGER NOT NULL,`number` INTEGER, `spot` INTEGER, "
                      "`name` TEXT, `latitude` REAL, `longitude` REAL,`terminal_type` TEXT, PRIMARY KEY(`uid`))")
            if self.logging:
                logging.info("Set up table: 'place_data'")
            c.execute("CREATE TABLE `city_data` ( `uid` INTEGER NOT NULL, `name` TEXT,`num_places` INTEGER, "
                      "`latitude` REAL, `longitude` REAL, PRIMARY KEY(`uid`) )")
            if self.logging:
                logging.info("Set up table: 'city_data'")
            c.execute("CREATE TABLE `domain_data` ( `domain` TEXT NOT NULL, `name` TEXT NOT NULL, "
                      "`country` TEXT NOT NULL, `latitude` REAL, `longitude` REAL, PRIMARY KEY(`domain`) )")
            if self.logging:
                logging.info("Set up table: 'domain_data'")
            c.execute("CREATE TABLE `places_cities_assignment` ( `place_uid` INTEGER NOT NULL, "
                      "`city_uid` INTEGER NOT NULL, UNIQUE (`place_uid`, `city_uid`) )")
            if self.logging:
                logging.info("Set up table: 'place_city_assignment'")
            c.execute("CREATE TABLE `cities_domains_assignment` ( `domain` TEXT NOT NULL, `city_uid` INTEGER, "
                      "UNIQUE ( `domain`, `city_uid`) ) ")
            if self.logging:
                logging.info("Set up table: 'cities_domain_assignment'")

    def fill_if_empty(self):
        """"Makes sure there is data in the master data db. If nothing exists, if will be filled."""
        c = self.conn.cursor()
        c.execute("SELECT '1' FROM places_data LIMIT 1")
        if len(c.fetchall()) < 1:
            self.update_db()

    def check_place(self, place_uid):
        """Returns true if the provided place_uid is valid"""
        c = self.conn.cursor()
        c.execute("SELECT 1 FROM places_data WHERE places_data.uid = ?", (place_uid,))
        return bool(c.fetchone())

    def check_city(self, city_uid):
        """"Returns true, if the provided city_uid is valid"""
        c = self.conn.cursor()
        c.execute("SELECT 1 FROM city_data WHERE city_data.uid = ?", (city_uid,))
        return bool(c.fetchone())

    def check_domain(self, domain_id):
        """"Returns true, if the provided domain is valid"""
        c = self.conn.cursor()
        c.execute("SELECT 1 FROM domain_data WHERE domain_data.domain = ?", (domain_id,))
        return bool(c.fetchone())

    def get_cities_from_domain(self, domain):
        """Returns a list of cities which belong to the provided short domain handle"""
        c = self.conn.cursor()
        c.execute("SELECT cities_domains_assignment.city_uid FROM cities_domains_assignment "
                  "WHERE cities_domains_assignment.domain = ?", (domain,))
        # get list of one element tuples as result
        res = c.fetchall()
        # unpack list of tuples to a list
        res_list = [x[0] for x in res]
        return res_list

    def get_places_from_city(self, city_uid):
        """Returns a list of places which belong to the provided city_uid"""
        c = self.conn.cursor()
        c.execute("SELECT places_cities_assignment.place_uid FROM places_cities_assignment "
                  "WHERE places_cities_assignment.city_uid = ?", (city_uid,))
        # get list of one element tuples as result
        res = c.fetchall()
        # unpack list of tuples to a list
        res_list = [x[0] for x in res]
        return res_list

    def get_places_from_domain(self, domain):
        """Returns a list of all places which belong to the proved short domain handle"""
        place_list = list()
        city_list = self.get_cities_from_domain(domain)
        for city in city_list:
            place_list.extend(self.get_places_from_city(city))
        return place_list

    def get_station_status(self, current=True):
        """"Returns an XML Tree and time of query for the latest status. If current == True,
        it will get the current status"""
        if current:
            self._download_station_status()
            self._parse_station_status()
        return self.status_xml, self.status_time

    def _download_station_status(self):
        """" Opens the Stations-status url and saves the result"""
        url = self.login_db.get_url("StationList")
        response = urllib.request.urlopen(url)
        self.status_xml_raw = response.read().decode()

    def _parse_station_status(self):
        """Returns an XML-object with the current status of all stations world-wide and a datetime of query as tuple"""
        success = False
        num_tries = 0

        # check if status file as been downloaded. If not, do so
        if not hasattr(self, "status_xml"):
            self._download_station_status()

        # Parse XML, an if necessary download again if file is corrupt,
        # often for corrupt files getting the time will go wrong
        while not success and num_tries < 10:
            try:
                self.status_xml = ElmTree.fromstring(self.status_xml_raw)
                # get the time of the query from the comment at the end of the file
                query_time_start_pos = self.status_xml_raw.find("<!--")
                query_time_end_pos = self.status_xml_raw.find("-->")
                time_string = self.status_xml_raw[query_time_start_pos + 5:query_time_end_pos - 1]
                self.status_time = datetime.datetime.strptime(time_string, "%d.%m.%Y %H:%M")
                success = True
            except (ElmTree.ParseError, ValueError):  # parsing of xml went wrong
                success = False
                num_tries += 1
                print("Problems Downloading Current Stations List Connection try ", num_tries - 1,
                      "failed. Will try again")
                self._download_station_status()

        assert isinstance(self.status_xml, ElmTree.Element)
        if not success:
            raise ValueError('Could not get Station Data or Parse received XML-File')

    def update_db(self):
        """"Updates the stations master data records"""
        self._download_station_status()
        self._parse_station_status()
        self._update_domain_table()
        self._update_city_table()
        self._update_cities_domain_assign_table()
        self._update_places_table()
        self._update_places_cities_assign_table()

    def print_master_data(self):
        """Prints the list of stations from the current-status-xml-file on screen"""
        for domain in self.status_xml:
            print(domain.attrib.get("country_name"), ": ", domain.attrib.get("name"))
            for city in domain:
                print("\n", city.attrib.get("name"), "; ", city.tag, "-uid:", city.attrib.get("uid"))
                for place in city:
                    print(place.attrib.get("name"), "; ", place.tag, "-uid:", place.attrib.get("uid"))
            print("----------------------------------------------------")

    def _check_status_integrity(self):
        try:
            assert isinstance(self.status_xml, ElmTree.ElementTree)
        except AssertionError:
            self._parse_station_status()

    def _update_city_table(self):
        """"Writes general city data from a provided xml-file to the database"""
        self._check_status_integrity()
        c = self.conn.cursor()
        for domain in self.status_xml:
            for city in domain:
                uid = city.attrib.get("uid")
                name = city.attrib.get("name")
                lat = city.attrib.get("lat")
                lng = city.attrib.get("lng")
                num_places = city.attrib.get("num_places")
                # check if record already exists and insert if not
                c.execute("SELECT 1 FROM city_data WHERE city_data.uid = ?", (uid,))
                if len(c.fetchall()) < 1:
                    c.execute("INSERT INTO city_data VALUES (?, ?, ?, ?, ?)", (uid, name, num_places, lat, lng))
                    logging.info("New Insert to city_data: uid %s - %s", uid, name)
        self.conn.commit()

    def _update_places_table(self):
        """Writes general stations data from a provided xml-file to the database """
        self._check_status_integrity()
        c = self.conn.cursor()
        for domain in self.status_xml:
            for city in domain:
                for place in city:
                    uid = place.attrib.get("uid")
                    number = place.attrib.get("number")
                    name = place.attrib.get("name")
                    spot = place.attrib.get("spot")
                    lat = place.attrib.get("lat")
                    lng = place.attrib.get("lng")
                    terminal_type = place.attrib.get("terminal_type")
                    # check if record already exists and insert if not
                    c.execute("SELECT 1 FROM places_data WHERE places_data.uid = ?", (uid,))
                    if len(c.fetchall()) < 1:
                        c.execute("INSERT INTO places_data VALUES (?, ?, ?, ?, ?, ?, ?)",
                                  (uid, number, spot, name, lat, lng, terminal_type))
                        logging.info("New Insert to places_data: uid %s - %s", uid, name)

        self.conn.commit()

    def _update_domain_table(self):
        """"Writes general country data from a provided xml-file to the database """
        self._check_status_integrity()
        c = self.conn.cursor()
        for domain in self.status_xml:
            domain_item = domain.attrib.get("domain")
            name = domain.attrib.get("name")
            country = domain.attrib.get("country")
            lat = domain.attrib.get("lat")
            lng = domain.attrib.get("lng")
            # check if record already exists and insert if not
            c.execute("SELECT 1 FROM domain_data WHERE domain_data.domain = ?", (domain_item,))
            if len(c.fetchall()) < 1:
                c.execute("INSERT INTO domain_data VALUES (?, ?, ?, ?, ?)",
                          (domain_item, name, country, lat, lng))
                logging.info("New Insert to domain_data: domain %s - %s", domain_item, name)
        self.conn.commit()

    def _update_cities_domain_assign_table(self):
        """"Writes teh relation between cities and countries from a provided xml-file to the database"""
        self._check_status_integrity()
        c = self.conn.cursor()
        for domain in self.status_xml:
            for city in domain:
                domain_item = domain.attrib.get("domain")
                city_uid = city.attrib.get("uid")
                c.execute("INSERT OR IGNORE INTO cities_domains_assignment VALUES (?,?)",
                          (domain_item, city_uid))
        self.conn.commit()

    def _update_places_cities_assign_table(self):
        """"Writes the relation between places and cities from a provided xml-file to the database """
        self._check_status_integrity()
        c = self.conn.cursor()
        for domain in self.status_xml:
            for city in domain:
                for place in city:
                    place_uid = place.attrib.get("uid")
                    city_uid = city.attrib.get("uid")
                    c.execute("INSERT OR IGNORE INTO places_cities_assignment VALUES (?, ?)",
                              (place_uid, city_uid))
        self.conn.commit()
