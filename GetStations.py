import urllib.request
import xml.etree.ElementTree as ElmTree
import sqlite3


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
    """Returns an XML-object with the current status of all stations world-wide"""
    url = get_url_from_db()
    response = urllib.request.urlopen(url)
    xml_tree = response.read().decode()
    return xml_tree


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

    #check if station_info table is present and return connection object
    try:
        c.execute("SELECT * FROM places_data LIMIT 1")
        return conn

    # if no station_info table, the database file did not exist. create station info and city infor table
    except sqlite3.OperationalError:
        pass



if __name__ == '__main__':
    xml_data = get_stations_status()
    print_xml_data(xml_data)
