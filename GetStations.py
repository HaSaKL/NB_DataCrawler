import urllib.request
import xml.etree.ElementTree as ElmTree
import sqlite3
import datetime


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


def get_stations_status():
    """Returns an XML-object with the current status of all stations world-wide and a datetime of query as tuple"""
    url = get_url_from_db()
    response = urllib.request.urlopen(url)
    xml_tree = response.read().decode()

    # get the time of the query from the comment at the end of the file
    query_time_start_pos = xml_tree.find("<!--")
    query_time_end_pos = xml_tree.find("-->")
    time_string = xml_tree[query_time_start_pos+5 : query_time_end_pos-1]
    time = datetime.datetime.strptime(time_string, "%d.%m.%Y %H:%M")

    return (xml_tree, time)


def print_xml_data(data):
    """Prints the list of stations from the current-status-xml-file on screen"""
    root = ElmTree.fromstring(data)
    print(root)
    for country in root:
        print(country.attrib.get("country_name"), ": ", country.attrib.get("name"))
        for city in country:
            print("\n", city.attrib.get("name"), "; ", city.tag, "-uid:", city.attrib.get("uid"))
            for place in city:
                print(place.attrib.get("name"), "; ", place.tag, "-uid:", place.attrib.get("uid"))
        print("----------------------------------------------------")


def connect_stations_db():
    """Returns a connection object to the bikes database after openen an existix or creating a new sqlite-file"""
    conn = sqlite3.connect("stations.db")
    c = conn.cursor()

    # check if station_info table is present and return connection object
    try:
        c.execute("SELECT uid FROM places_data LIMIT 1")
    # if no station_info table, the database file did not exist. create station info and city infor table
    except sqlite3.OperationalError:
        c.execute("CREATE TABLE `places_data` (`uid` INTEGER NOT NULL,`number` INTEGER, `spot` INTEGER, "
                  "`name` TEXT, `latitude` REAL, `longitude` REAL,`terminal_type` TEXT, PRIMARY KEY(`uid`))")
        c.execute("CREATE TABLE `city_data` ( `uid` INTEGER NOT NULL, `name` TEXT,`num_places` INTEGER, "
                  "`latitude` REAL, `longitude` REAL, PRIMARY KEY(`uid`) )")
        c.execute("CREATE TABLE `country_data` ( `domain` TEXT NOT NULL, `name` TEXT NOT NULL, "
                  "`country` TEXT NOT NULL, `latitute` REAL, `longitude` REAL, PRIMARY KEY(`domain`) )")
    finally:
        return conn


def update_city_info_from_xml(data, conn):
    """"Writes general city data from a provided xml-file to the database"""
    c = conn.cursor()
    root = ElmTree.fromstring(data)
    for country in root:
        for city in country:
            uid = city.attrib.get("uid")
            name = city.attrib.get("name")
            lat = city.attrib.get("lat")
            lng = city.attrib.get("lng")
            num_places = city.attrib.get("num_places")
            c.execute("INSERT OR IGNORE INTO city_data VALUES (?, ?, ?, ?, ?)", (uid, name, num_places, lat, lng))
    conn.commit()


def update_station_info_from_xml(data, conn):
    """Writes general stations data from a provided xml-file to the database """
    c = conn.cursor()
    root = ElmTree.fromstring(data)
    for country in root:
        for city in country:
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


def update_country_info_from_xml(data, conn):
    """"Writes general country data from a provided xml-file to the database """
    c = conn.cursor()
    root = ElmTree.fromstring(data)
    for country_item in root:
        domain = country_item.attrib.get("domain")
        name = country_item.attrib.get("name")
        country = country_item.attrib.get("country")
        lat = country_item.attrib.get("lat")
        lng = country_item.attrib.get("lng")
        c.execute("INSERT OR IGNORE INTO country_data VALUES (?, ?, ?, ?, ?)",
                  (domain, name, country, lat, lng))
    conn.commit()


def init_DB():
    """Reads the general stations data from the web and writes it to the database"""
    print("Opening web connection and downloading stations data")
    xml_data, time = get_stations_status()
    print("done.")

    print("Time of query: ", time)

    print("Writing City Data to Database...")
    conn = connect_stations_db()
    update_city_info_from_xml(xml_data, conn)
    print("Writing Stations Data to Database...")
    update_station_info_from_xml(xml_data, conn)
    print("Writing Country Data to Database...")
    update_country_info_from_xml(xml_data, conn)
    print("done.")


def update_DB():
    """"Updates the stations master data records"""
    init_DB()


if __name__ == '__main__':
    xml_data, time = get_stations_status()
    print_xml_data(xml_data)
    print("Time of query: ",time)
