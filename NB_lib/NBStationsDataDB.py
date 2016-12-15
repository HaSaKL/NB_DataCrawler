import sqlite3
import xml.etree.ElementTree as ElmTree

from NB_lib import NBMasterDataDB


class NBStationsDataDB:
    """Class which defines an abstract interface to the master data base"""

    def __init__(self, transactions_db_name="stations_transactions.db", master_data_db_name="stations_master.db",
                 login_data_db_name="login.db", log_file="db_log.log"):
        """"Creates a database connection at initialization and establishes base DB-structure if necessary,
               also creates an NBMasterDataDB Object and fills it"""
        self.master_db = NBMasterDataDB.NBMasterDataDB(login_data_db_name=login_data_db_name,
                                                       master_data_db_name=master_data_db_name,
                                                       log_file=log_file)
        self.master_db.fill_if_empty()

        self.conn = sqlite3.connect(transactions_db_name)
        c = self.conn.cursor()
        # check if database contains a table with transaction data; create table if necessary
        # noinspection SqlResolve
        c.execute("SELECT 1 FROM sqlite_master WHERE tbl_name = 'stations_fill' AND type = 'table'")
        if len(c.fetchall()) < 1:
            c.execute("CREATE TABLE `stations_fill` ( `timestamp` INTEGER NOT NULL, `place_uid` INTEGER NOT NULL, "
                      "`bikes` INTEGER NOT NULL, UNIQUE ( `place_uid`, `timestamp`) ) ")

    def add_state(self, status_xml, status_time):
        """"Adds a state defined by an status_xml and a time to the database"""
        c = self.conn.cursor()

        for domain in status_xml:
            for city in domain:
                for place in city:
                    place_uid = place.attrib.get("uid")
                    bikes = place.attrib.get("bikes")
                    c.execute("INSERT OR IGNORE INTO stations_fill VALUES (?, ?, ?)",
                              (int(status_time.timestamp()), place_uid, bikes))
        self.conn.commit()

    def add_current_state(self, places_list = []):
        """Downloads the current state and adds it to the database, if station list is provided,
        only add stations from list"""

        # get snapshot of station  and make sure it is a valid ElmTree
        status_xml, status_time = self.master_db.get_station_status(True)
        assert isinstance(status_xml, ElmTree.Element)

        if not len(places_list) > 0:
            # if no places are specified, add all places to DB
            self.add_state(status_xml, status_time)
        else:
            # if places are specified, go through entire results file but only add data for the places specified
            c = self.conn.cursor()

            for domain in status_xml:
                for city in domain:
                    for place in city:
                        place_uid = place.attrib.get("uid")
                        bikes = place.attrib.get("bikes")
                        if int(place_uid) in places_list:
                            c.execute("INSERT OR IGNORE INTO stations_fill VALUES (?, ?, ?)",
                                      (int(status_time.timestamp()), place_uid, bikes))
            self.conn.commit()