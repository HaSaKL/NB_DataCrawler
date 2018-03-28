import sqlite3

master_data_db_name="stations_master.db"

# connect to db
conn = sqlite3.connect(master_data_db_name)
c = conn.cursor()

# add database migrations capability to database
c.execute("CREATE TABLE 'admin_migrations' ('id' INTEGER PRIMARY KEY , 'name' TEXT NOT NULL"
          ", 'applied' TIMESTAMP NOT NULL)")

c.execute("INSERT INTO 'admin_migrations' VALUES (NULL,?,current_date)", ("0000_INITIAL_DATABASE",))

# insert first migration: Alter Table Places Data to hold last- and first-seen entries
c.execute("ALTER TABLE 'places_data' ADD COLUMN 'first_seen' TIMESTAMP")
c.execute("UPDATE 'places_data' SET 'first_seen' = current_date")
c.execute("ALTER TABLE 'places_data' ADD COLUMN 'last_seen' TIMESTAMP")
c.execute("UPDATE 'places_data' SET 'last_seen' = current_date")

c.execute("INSERT INTO 'admin_migrations' VALUES (NULL,?,current_date)", ("0001_PALACE_FIRST_SEEN_LAST_SEEN",))

# commit and close db-connection
conn.commit()
conn.close()





