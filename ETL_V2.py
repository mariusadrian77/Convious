import requests
import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
from psycopg2 import pool

# VARIABLES

# Load environment variables from the .env file
load_dotenv()

# Fetch the Holiday API key and database URL from the .env file
HOLIDAYAPI_URL = os.getenv("HOLIDAY_API_KEY")
POSTGRESQL_URL = os.getenv("POSTGRESQL_KEY")

# Parse the database URL to extract connection parameters
postgressql_url = urlparse(POSTGRESQL_URL)
conn_params = {
    'dbname': postgressql_url.path[1:],    # Extracts the database name after '/'
    'user': postgressql_url.username,       # Extracts the username
    'password': postgressql_url.password,   # Extracts the password
    'host': postgressql_url.hostname,       # Extracts the host
    'port': postgressql_url.port            # Extracts the port
}

# Initialize the connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,  # Adjust max connections based on your needs
    **conn_params
)

# List of locations with their corresponding country codes and subdivision codes
locations = [
    {"location_id": "9415913d-fffa-41f9-9323-6d62e6100a31", "country_code": "NL", "subdivision_code": "NL-FL", "name": "Flevoland"},
    # Add more locations here...
]

# Define the base URL for the API
holidayapi_url = "https://holidayapi.com/v1/holidays"

# FUNCTIONS

# Function to fetch holiday data for a specific location
def fetch_holiday_data(country_code, year, subdivision_code=None):
    params = {
        'key': HOLIDAYAPI_URL,
        'country': country_code,
        'year': year,
        'pretty': 'true'  # Optional: format the response for readability
    }
    if subdivision_code:
        params['subdivision'] = subdivision_code
    
    response = requests.get(holidayapi_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {country_code}-{subdivision_code}. Status code: {response.status_code}")
        return None

# Function to get the latest version number for a location
def get_latest_version(cursor, location_id):
    cursor.execute("""
        SELECT COALESCE(MAX(version), 0) FROM location_mapping WHERE location_id = %s;
    """, (location_id,))
    return cursor.fetchone()[0]

# Function to fetch the latest location data for comparison
def get_latest_location_data(cursor, location_id):
    cursor.execute("""
        SELECT country_code, subdivision_code, name FROM location_mapping 
        WHERE location_id = %s ORDER BY version DESC LIMIT 1;
    """, (location_id,))
    return cursor.fetchone()

# Function to check if data has changed
def has_location_changed(cursor, location):
    latest_data = get_latest_location_data(cursor, location['location_id'])
    if latest_data:
        return (
            latest_data[0] != location['country_code'] or
            latest_data[1] != location['subdivision_code'] or
            latest_data[2] != location['name']
        )
    return True  # If there's no previous data, consider it changed

# Function to upsert locations into the database with version control
def upsert_location(cursor, location):
    if has_location_changed(cursor, location):
        latest_version = get_latest_version(cursor, location['location_id'])
        new_version = latest_version + 1

        cursor.execute("""
            INSERT INTO location_mapping (location_id, country_code, subdivision_code, name, version)
            VALUES (%s, %s, %s, %s, %s);
            """, (
                location['location_id'], 
                location['country_code'], 
                location['subdivision_code'], 
                location['name'],
                new_version
            ))
        print(f"Location data inserted for {location['name']} with version {new_version}.")

# Function to insert holiday data into the database
def insert_holiday_data(cursor, location_id, holiday_data):
    for holiday in holiday_data['holidays']:
        cursor.execute("""
            INSERT INTO holidays (location_id, date, name, observed, public)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            location_id,
            holiday['date'],
            holiday['name'],
            holiday['observed'],
            holiday['public']
        ))

# MAIN FUNCTION

def main():
    # Fetch and print holiday data for all locations
    year = 2023  # Free version ONLY allows fetching data for the previous year

    # Use a connection from the connection pool
    conn = connection_pool.getconn()

    try:
        # Create a cursor object to execute SQL queries
        cursor = conn.cursor()
        print("Server connection initialized!")

        for location in locations:
            country_code = location["country_code"]
            subdivision_code = location["subdivision_code"]
            location_id = location["location_id"]

            print(f"Fetching holiday data for {country_code}-{subdivision_code}...")
            holiday_data = fetch_holiday_data(country_code, year, subdivision_code)
            print("Holiday data fetching complete!")

            if holiday_data:
                # Insert holiday data
                insert_holiday_data(cursor, location_id, holiday_data)
                
                print("Holiday data inserted successfully!")
                
                # Upsert location data with versioning
                upsert_location(cursor, location)
                
                print("Location data upserted successfully!")


        print("Location data updated and holiday data upserted successfully!")
        # Commit the transaction to save changes
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Rollback in case of error

    finally:
        # Close the cursor and release the connection back to the pool
        cursor.close()
        connection_pool.putconn(conn)

    # Close the connection pool when done
    connection_pool.closeall()

# Check if the script is being run directly
if __name__ == "__main__":
    main()
