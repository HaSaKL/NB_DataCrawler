import sqlite3
import NBMasterDataDB
import xml.etree.ElementTree as ElmTree


class NBStationsDataDB:
    """Class which defines an abstract interface to the master data base"""

    def __init__(self, transactions_db_name="stations_transactions.db", master_data_db_name="stations_master.db",
                 login_data_db_name="login.db"):
        """"Creates a database connection at initialization and establishes base DB-structure if necessary,
               also creates an NBMasterDataDB Object"""
        self.master_db = NBMasterDataDB.NBMasterDataDB(master_data_db_name, login_data_db_name)
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
        assert isinstance(status_xml, ElmTree.Element)
        for domain in status_xml:
            for city in domain:
                for place in city:
                    place_uid = place.attrib.get("uid")
                    bikes = place.attrib.get("bikes")
                    c.execute("INSERT OR IGNORE INTO stations_fill VALUES (?, ?, ?)",
                              (int(status_time.timestamp()), place_uid, bikes))
        self.conn.commit()

    def add_current_state(self):
        """Downloads the current state and adds it to the database"""
        status_xml, status_time = self.master_db.get_station_status(True)
        self.add_state(status_xml, status_time)
