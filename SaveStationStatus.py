import NBCLI
import NBStationsDataDB

if __name__ == '__main__':

    # parse command line arguments and read config files
    config = NBCLI.NBCLI()

    # open database (this will also open all other depending databases)
    stations_db = NBStationsDataDB.NBStationsDataDB(transactions_db_name=config.stations_transactions_db_file,
                                                    master_data_db_name=config.stations_master_db_file,
                                                    login_data_db_name=config.login_db_file)

    # write info from relevant stations to database
    stations_db.add_current_state(config.places_list)
