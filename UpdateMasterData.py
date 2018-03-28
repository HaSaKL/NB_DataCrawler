from NB_lib import NBCLI, NBMasterDataDB
import smtplib
from email.mime.text import MIMEText
import datetime

if __name__ == '__main__':

    # parse command line arguments and read config files
    config = NBCLI.NBCLI()

    # open database
    master_data = NBMasterDataDB.NBMasterDataDB(master_data_db_name=config.stations_master_db_file,
                                                login_data_db_name=config.login_db_file,
                                                log_file=config.cmdl_args.logfile,
                                                stations_master_migration=config.stations_master_migration)

    # check database, update if necessary, and detect changes
    changes_str = master_data.update_db()

    # if changes occurred, send an email with the last entries of the logfile
    if len(changes_str) > 0 and config.log_email_status:
        # make email
        msg = MIMEText(changes_str)
        msg['Subject'] = "[{}] {}".format(config.log_email_prefix, datetime.datetime.now().strftime("%Y-%m-%d %H:%Mh"))
        msg['From'] = config.log_email_from
        msg['To'] = config.log_email_to

        # send email
        s = smtplib.SMTP(config.log_email_smtp)
        s.send_message(msg)
        s.quit()
