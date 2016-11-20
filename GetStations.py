import urllib.request
import xml.etree.ElementTree as ET

getStationsURL = "https://nextbike.net/maps/nextbike-official.xml"

if __name__ == '__main__':
    print("opening URL")
    response = urllib.request.urlopen(getStationsURL)
    xml_data = response.read()
    xml_data = xml_data.decode()
    root = ET.fromstring(xml_data)

    print(root)

    for country in root:
        print(country.attrib.get("country_name"), ": ", country.attrib.get("name"))
        for city in country:
            print("\n", city.attrib.get("name" ), "; ",  city.tag, "-uid:", city.attrib.get("uid"))
            for place in city:
                print(place.attrib.get("name"), "; ", place.tag, "-uid:", place.attrib.get("uid"))
        print("----------------------------------------------------")