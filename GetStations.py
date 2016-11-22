import urllib.request
import xml.etree.ElementTree as ElmTree
import sqlite3


def get_url():
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


if __name__ == '__main__':
    getStationsURL = get_url()

    print("opening URL")
    response = urllib.request.urlopen(getStationsURL)
    xml_data = response.read()
    xml_data = xml_data.decode()
    root = ElmTree.fromstring(xml_data)

    print(root)

    for country in root:
        print(country.attrib.get("country_name"), ": ", country.attrib.get("name"))
        for city in country:
            print("\n", city.attrib.get("name"), "; ",  city.tag, "-uid:", city.attrib.get("uid"))
            for place in city:
                print(place.attrib.get("name"), "; ", place.tag, "-uid:", place.attrib.get("uid"))
        print("----------------------------------------------------")
