import urllib.request
import xml.etree.ElementTree as ElmTree
import sqlite3
import datetime
import argparse
import configparser
import sys


def get_url_from_db():
    """Returns the URL to receive general station info and current station occupation from the Database"""
    try:
        conn = sqlite3.connect("login.db")
        c = conn.cursor()
        c.execute("SELECT urls.url FROM urls WHERE urls.name = 'StationList'")
        try:
            url = c.fetchone()[0]
            conn.close()
            return url
        except TypeError:
            print("ERROR: Could not fetch URL from Database login.db - Is it there?")
            exit()
    except sqlite3.Error:
        print("Error Opening Database login.db")
        exit()


def get_station_status():
    """" Opens the Stations-status url and return the downloaded the xml file"""
    url = get_url_from_db()
    response = urllib.request.urlopen(url)
    xml_string = response.read().decode()
    return xml_string


def get_and_parse_station_status():
    """Returns an XML-object with the current status of all stations world-wide and a datetime of query as tuple"""
    success = False
    num_tries = 0
    xml_root = None

    # Get Information from the Web
    xml_string = get_station_status()
    assert isinstance(xml_string, str)

    # Parse XML, an if necessary retry
    while not success and num_tries < 10:
        try:
            xml_root = ElmTree.fromstring(xml_string)
            success = True
        except ElmTree.ParseError:
            success = False
            num_tries += 1
            print("Connection try ", num_tries - 1, "failed. Will try again")
            xml_string = get_station_status()
            assert isinstance(xml_string, str)

    assert isinstance(xml_root, ElmTree.Element)
    if not success:
        raise ValueError('Could not get Station Data or Parse received XML-File')

    # get the time of the query from the comment at the end of the file
    query_time_start_pos = xml_string.find("<!--")
    query_time_end_pos = xml_string.find("-->")
    time_string = xml_string[query_time_start_pos+5:query_time_end_pos-1]
    query_time = datetime.datetime.strptime(time_string, "%d.%m.%Y %H:%M")

    return xml_root, query_time


def print_xml_data(xml_root):
    """Prints the list of stations from the current-status-xml-file on screen"""
    print(xml_root)
    for domain in xml_root:
        print(domain.attrib.get("country_name"), ": ", domain.attrib.get("name"))
        for city in domain:
            print("\n", city.attrib.get("name"), "; ", city.tag, "-uid:", city.attrib.get("uid"))
            for place in city:
                print(place.attrib.get("name"), "; ", place.tag, "-uid:", place.attrib.get("uid"))
        print("----------------------------------------------------")


def connect_stations_master_db():
    """Returns a connection object to the bikes database after opened an existing or creating a new sqlite-file"""
    conn = sqlite3.connect("stations_master.db")
    c = conn.cursor()

    # check if places_data table is present and return connection object
    try:
        c.execute("SELECT `uid` FROM `places_data` LIMIT 1")
    # if no places_data table, the database file did not exist. create station info, city info, domain info an all
    # linking table and all other tables
    except sqlite3.OperationalError:
        c.execute("CREATE TABLE `places_data` (`uid` INTEGER NOT NULL,`number` INTEGER, `spot` INTEGER, "
                  "`name` TEXT, `latitude` REAL, `longitude` REAL,`terminal_type` TEXT, PRIMARY KEY(`uid`))")
        c.execute("CREATE TABLE `city_data` ( `uid` INTEGER NOT NULL, `name` TEXT,`num_places` INTEGER, "
                  "`latitude` REAL, `longitude` REAL, PRIMARY KEY(`uid`) )")
        c.execute("CREATE TABLE `domain_data` ( `domain` TEXT NOT NULL, `name` TEXT NOT NULL, "
                  "`country` TEXT NOT NULL, `latitude` REAL, `longitude` REAL, PRIMARY KEY(`domain`) )")
        c.execute("CREATE TABLE `places_cities_assignment` ( `place_uid` INTEGER NOT NULL, "
                  "`city_uid` INTEGER NOT NULL, UNIQUE (`place_uid`, `city_uid`) )")
        c.execute("CREATE TABLE `cities_domains_assignment` ( `domain` TEXT NOT NULL, `city_uid` INTEGER, "
                  "UNIQUE ( `domain`, `city_uid`) ) ")
    finally:
        return conn


def update_city_info_from_xml(xml_root, conn):
    """"Writes general city data from a provided xml-file to the database"""
    c = conn.cursor()
    for domain in xml_root:
        for city in domain:
            uid = city.attrib.get("uid")
            name = city.attrib.get("name")
            lat = city.attrib.get("lat")
            lng = city.attrib.get("lng")
            num_places = city.attrib.get("num_places")
            c.execute("INSERT OR IGNORE INTO city_data VALUES (?, ?, ?, ?, ?)", (uid, name, num_places, lat, lng))
    conn.commit()


def update_station_info_from_xml(xml_root, conn):
    """Writes general stations data from a provided xml-file to the database """
    c = conn.cursor()
    for domain in xml_root:
        for city in domain:
            for place in city:
                uid = place.attrib.get("uid")
                number = place.attrib.get("number")
                name = place.attrib.get("name")
                spot = place.attrib.get("spot")
                lat = place.attrib.get("lat")
                lng = place.attrib.get("lng")
                terminal_type = place.attrib.get("terminal_type")
                c.execute("INSERT OR IGNORE INTO places_data VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (uid, number, spot, name, lat, lng, terminal_type))
    conn.commit()


def update_domain_info_from_xml(xml_root, conn):
    """"Writes general country data from a provided xml-file to the database """
    c = conn.cursor()
    for domain in xml_root:
        domain_item = domain.attrib.get("domain")
        name = domain.attrib.get("name")
        country = domain.attrib.get("country")
        lat = domain.attrib.get("lat")
        lng = domain.attrib.get("lng")
        c.execute("INSERT OR IGNORE INTO domain_data VALUES (?, ?, ?, ?, ?)",
                  (domain_item, name, country, lat, lng))
    conn.commit()


def update_cities_domain_assign_info_from_xml(xml_root, conn):
    """"Writes teh relation between cities and countries from a provided xml-file to the database"""
    c = conn.cursor()
    for domain in xml_root:
        for city in domain:
            domain_item = domain.attrib.get("domain")
            city_uid = city.attrib.get("uid")
            c.execute("INSERT OR IGNORE INTO cities_domains_assignment VALUES (?,?)",
                      (domain_item, city_uid))
    conn.commit()


def update_places_cities_assign_info_from_xml(xml_root, conn):
    """"Writes the relation between places and cities from a provided xml-file to the database """
    c = conn.cursor()
    for domain in xml_root:
        for city in domain:
            for place in city:
                place_uid = place.attrib.get("uid")
                city_uid = city.attrib.get("uid")
                c.execute("INSERT OR IGNORE INTO places_cities_assignment VALUES (?, ?)",
                          (place_uid, city_uid))
    conn.commit()


def init_db():
    """Reads the general stations data from the web and writes it to the database"""
    print("Opening web connection and downloading stations data")
    query_xml_root, query_time = get_and_parse_station_status()
    print("done.")

    print("Time of query: ", query_time)

    print("Writing City Data to Database...")
    conn = connect_stations_master_db()
    update_city_info_from_xml(query_xml_root, conn)
    print("Writing Stations Data to Database...")
    update_station_info_from_xml(query_xml_root, conn)
    print("Writing Country Data to Database...")
    update_domain_info_from_xml(query_xml_root, conn)
    print("Writing places to city assignments...")
    update_places_cities_assign_info_from_xml(query_xml_root, conn)
    print("Writing cities to domain assignments...")
    update_cities_domain_assign_info_from_xml(query_xml_root, conn)
    print("done.")


def update_master_data_db():
    """"Updates the stations master data records"""
    init_db()


def connect_stations_transfer_db():
    """"Returns a connection object to the bikes stations status database after opening an existing of creating a new
    sqlite-file"""
    conn = sqlite3.connect("stations_transactions.db")
    c = conn.cursor()

    # check if station status does exist. If not, make the table
    try:
        c.execute("SELECT * FROM`stations_fill` LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("CREATE TABLE `stations_fill` ( `timestamp` INTEGER NOT NULL, `place_uid` INTEGER NOT NULL, "
                  "`bikes` INTEGER NOT NULL, UNIQUE ( `place_uid`, `timestamp`) ) ")
    finally:
        return conn


def add_current_station_info():
    """"Reads the current station status and writes query result into the transaction database"""
    query_xml_root, query_time = get_and_parse_station_status()
    conn = connect_stations_transfer_db()
    c = conn.cursor()

    print("Getting and writing data from ", query_time.timestamp())

    # noinspection PyTypeChecker
    for domain in query_xml_root:
        for city in domain:
            for place in city:
                place_uid = place.attrib.get("uid")
                bikes = place.attrib.get("bikes")
                c.execute("INSERT OR IGNORE INTO `stations_fill` VALUES (?, ?, ?)",
                          (int(query_time.timestamp()), place_uid, bikes))
    conn.commit()


def get_place_from_city(conn, city_uid):
    """Returns a list of places which belong to the provided city_uid"""
    c = conn.cursor()
    c.execute("SELECT places_cities_assignment.place_uid FROM places_cities_assignment "
              "WHERE places_cities_assignment.city_uid = ?", (city_uid,))
    # get list of one element tuples as result
    res = c.fetchall()
    # unpack list of tuples to list
    res_list = [x[0] for x in res]
    return res_list


def get_city_from_domain(conn, domain):
    """Returns a list of cities which belong to the provided domain"""
    c = conn.cursor()
    c.execute("SELECT cities_domains_assignment.city_uid FROM cities_domains_assignment "
              "WHERE cities_domains_assignment.domain = ?", (domain,))
    # get list of one element tuples as result
    res = c.fetchall()
    # unpack list of tuples to list
    res_list = [x[0] for x in res]
    return res_list


def check_place(conn, place_uid):
    """Returns true if the provided place_uid is valid"""
    c = conn.cursor()
    c.execute("SELECT 1 FROM places_data WHERE places_data.uid = ?", (place_uid,))
    return bool(c.fetchone())


def check_city(conn, city_uid):
    """"Returns true, if the provided city_uid is valid"""
    c = conn.cursor()
    c.execute("SELECT 1 FROM city_data WHERE city_data.uid = ?", (city_uid,))
    return bool(c.fetchone())


def check_domain(conn, domain_id):
    """"Returns true, if the provided domain is valid"""
    c = conn.cursor()
    c.execute("SELECT 1 FROM domain_data WHERE domain_data.domain = ?", (domain_id,))
    return bool(c.fetchone())


def read_places_uid(config_file, conn):
    """"Takes the location/name.ini of a config file and reads uid of the places mentioned in the file"""
    # make parser and open places config file
    config = configparser.ConfigParser()
    try:
        assert config_file.endswith('.ini')
        assert config.read(config_file)
    except AssertionError:
        sys.exit("Wrong Configuration File. File should exist and end with .ini")

    # get a list of all stations
    # fixme: needs refactoring very badly
    places_list = list()
    for section in config.sections():
        if section == "domain":
            for domain in config["domain"].values():
                if check_domain(conn, domain):
                    city_in_domain = get_city_from_domain(conn, domain)
                    for city in city_in_domain:
                        if check_city(conn, city):
                            place_in_city = get_place_from_city(conn,city)
                            for place in place_in_city:
                                places_list.append(place)

        elif section == "city_uid":
            for city in config["city_uid"].values():
                if check_city(conn, city):
                    place_in_city = get_place_from_city(conn, city)
                    for place in place_in_city:
                        places_list.append(place)

        elif section == "place_uid":
            for place in config["place_uid"].values():
                if check_place(conn, place):
                    places_list.append(place)

    # return list with unique places
    return list(set(places_list))


def ui_cli():
    """"Parses the command line options an returns the corresponding namespace"""
    parser = argparse.ArgumentParser(description="Get Data from NextBike Servers and Save it to a Database.")
    parser.add_argument("-p", "--places", type=str, default="places.ini", help="an ini-File with places")
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    db_conn = connect_stations_master_db()
    args = ui_cli()
    place_list = read_places_uid(args.places, db_conn)
    print(place_list)
    print(len(place_list))
