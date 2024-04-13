import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# Update connection string information
host = "udacitydemoc2.postgres.database.azure.com"
user = "udacity"
password = "@Ninda17071988"
sslmode = 'disable'  # Change to 'require' if SSL is needed

# Connect to the default database
conn_string = f"host={host} user={user} dbname=postgres password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Create cursor
cursor = conn.cursor()

# Drop and create a new database
cursor.execute('DROP DATABASE IF EXISTS udacityproject')
cursor.execute("CREATE DATABASE udacityproject")
conn.commit()
cursor.close()
conn.close()

# Reconnect to the new database
dbname = "udacityproject"
conn_string = f"host={host} user={user} dbname={dbname} password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# Helper functions
def drop_recreate(c, tablename, create_statement):
    c.execute(f"DROP TABLE IF EXISTS {tablename};")
    c.execute(create_statement)
    print(f"Finished creating table {tablename}")

def populate_table(c, filename, tablename):
    with open(filename, 'r') as f:
        try:
            c.copy_from(f, tablename, sep=",", null="")  # Using null values if present in CSV
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            conn.rollback()
            return 1  # Indicating error to stop further execution if needed
    print(f"Finished populating {tablename}")

# Define table structures and associated CSV file paths
tables = {
    "rider": ("./data/riders.csv", "CREATE TABLE rider (rider_id INTEGER PRIMARY KEY, first VARCHAR(50), last VARCHAR(50), address VARCHAR(100), birthday DATE, account_start_date DATE, account_end_date DATE, is_member BOOLEAN);"),
    "payment": ("./data/payments.csv", "CREATE TABLE payment (payment_id INTEGER PRIMARY KEY, date DATE, amount MONEY, rider_id INTEGER);"),
    "station": ("./data/stations.csv", "CREATE TABLE station (station_id VARCHAR(50) PRIMARY KEY, name VARCHAR(75), latitude FLOAT, longitude FLOAT);"),
    "trip": ("./data/trips.csv", "CREATE TABLE trip (trip_id VARCHAR(50) PRIMARY KEY, rideable_type VARCHAR(75), start_at TIMESTAMP, ended_at TIMESTAMP, start_station_id VARCHAR(50), end_station_id VARCHAR(50), rider_id INTEGER);")
}

# Process each table
for table_name, (file_path, create_statement) in tables.items():
    drop_recreate(cursor, table_name, create_statement)
    if os.path.exists(file_path):
        populate_table(cursor, file_path, table_name)
    else:
        print(f"File not found: {file_path}")

# Clean up
conn.commit()
cursor.close()
conn.close()

print("All done!")
