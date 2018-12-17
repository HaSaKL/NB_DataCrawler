# for accessing the database and the CLI-Interface
from NB_lib import NBStationsDataDB, NBCLI
from datetime import datetime

# for some file operations
import os

# for xml parsing
import xml.etree.ElementTree as ElmTree


def parse_file(file, bike_status_db):
    # get xml data from datafile
    try:
        xml_tree = ElmTree.parse(file)
        status_xml = xml_tree.getroot()
    except (ElmTree.ParseError, ValueError):
        print("Could not parse XML-File", file)
        return

    # get time / place info from filename
    try:
        status_time = datetime.strptime(file[-21:-4].replace(' ', '0'), "%Y-%m-%d-%Hh%Mm")
    except ValueError:
        print("Could not convert filename '", file, "' to datetime")
        return

    # add data and time data if data from file was parsed
    bike_status_db.add_state_country_level(status_xml, status_time)
    print("File ", file, " parsed and data saved in DB.")


if __name__ == '__main__':
    # parse command line arguments and read config files
    config = NBCLI.NBCLI()

    # open database (this will also open all other depending databases)
    stations_db = NBStationsDataDB.NBStationsDataDB(transactions_db_name=config.stations_transactions_db_file,
                                                    master_data_db_name=config.stations_master_db_file,
                                                    login_data_db_name=config.login_db_file,
                                                    log_file=config.cmdl_args.logfile)

    # iteratively open files, get info from file names and save to db
    path = config.cmdl_args.datadir
    for filename in os.listdir(path):
        parse_file(os.path.join(path, filename), stations_db)
