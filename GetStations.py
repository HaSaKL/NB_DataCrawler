import urllib.request
import xml.etree.ElementTree as ElmTree
import sqlite3


def get_url_from_db():
    """Returns the URL to receive general station info and current station occupation from the Database"""
    try:
        conn = sqlite3.connect("login.db")
        c = conn.cursor()
        c.execute("SELECT urls.url FROM urls WHERE urls.name = 'StationList'")
        url = c.fetchone()[0]
        conn.close()
        return url
    except sqlite3.Error:
        print("Error getting URL from Database")


def get_stations_status():
    """Returns an XML-object with the current status of all stations world-wide"""
    url = get_url_from_db()
    response = urllib.request.urlopen(url)
    xml_tree = response.read().decode()
    return xml_tree


def print_xml_data(data):
    root = ElmTree.fromstring(data)
    print(root)
    for country in root:
        print(country.attrib.get("country_name"), ": ", country.attrib.get("name"))
        for city in country:
            print("\n", city.attrib.get("name"), "; ", city.tag, "-uid:", city.attrib.get("uid"))
            for place in city:
                print(place.attrib.get("name"), "; ", place.tag, "-uid:", place.attrib.get("uid"))
        print("----------------------------------------------------")


if __name__ == '__main__':
    xml_data = get_stations_status()
    print_xml_data(xml_data)
