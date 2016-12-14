import sqlite3
import sys


class NBLoginDB:
    """Class to define an abstract interface to the Login Database"""

    def __init__(self, databasename="login.db"):
        """"Creates a database connection at initialization """
        self.conn = sqlite3.connect(databasename)

    def get_url(self, url_type):
        """Returns the URL to receive general station info and current station occupation from the Database"""
        if url_type == "StationList":
            return self._get_station_list_url()
        else:
            print("ERROR: Trying to get an unknown URL-Type")
            sys.exit()

    def get_apikey(self):
        pass

    def get_login(self):
        pass

    def _get_station_list_url(self):
        try:
            c = self.conn.cursor()
            c.execute("SELECT urls.url FROM urls WHERE urls.name = 'StationList'")
            try:
                url = c.fetchone()[0]
                return url
            except TypeError:
                print("ERROR: dCould not fetch URL from Database login.db - Is it there?")
                sys.exit()
        except sqlite3.Error:
            print("Error Opening Database login.db")
            sys.exit()
