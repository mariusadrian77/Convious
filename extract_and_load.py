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
    {"location_id": "432d10d2-1a7c-4bbd-abd2-85075fb19c71", "country_code": "NL", "subdivision_code": "NL-GR", "name": "Groningen"},
    {"location_id": "a2d0d6fd-3a18-4f58-ac36-d2e56bf71a46", "country_code": "GB", "subdivision_code": "GB-NSM", "name": "North Somerset"},
    {"location_id": "ab5df8c0-dfe7-4ca3-a9e4-c77f93e551a7", "country_code": "NL", "subdivision_code": "NL-ZH", "name": "South Holland"},
    {"location_id": "fdbf55b4-1b97-43a8-a096-a71d0b9d6940", "country_code": "GB", "subdivision_code": "GB-WLL", "name": "Wells"},
    {"location_id": "d9a11093-b1a4-4c1a-9e2a-7cc951b55a32", "country_code": "NL", "subdivision_code": "NL-DR", "name": "Drenthe"},
    {"location_id": "4f2c9d63-73b3-40f4-892d-136599854b87", "country_code": "NL", "subdivision_code": "NL-FR", "name": "Friesland"},
    {"location_id": "423ff765-83ac-472c-9ef7-b3a592696711", "country_code": "NL", "subdivision_code": "NL-UT", "name": "Utrecht"},
    {"location_id": "5770db11-e7bf-4044-b54c-d49f69e947ec", "country_code": "GB", "subdivision_code": "GB-SOM", "name": "Somerset"},
    {"location_id": "d70aed5a-3960-44fa-9c08-f725c2b03ce8", "country_code": "NL", "subdivision_code": "NL-NH", "name": "North Holland"},
    {"location_id": "e826584c-c32b-4ca1-835f-b7d7416f2958", "country_code": "NL", "subdivision_code": "NL-LI", "name": "Limburg"},
    {"location_id": "e40a0514-03e7-4d28-b7be-38c18a5ae73c", "country_code": "GB", "subdivision_code": "GB-WRT", "name": "Warrington"},
    {"location_id": "4c8eb5ba-0140-4b48-924b-899112abe562", "country_code": "NL", "subdivision_code": "NL-GE", "name": "Gelderland"}
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

# Function to upsert locations into the database
def upsert_location(cursor, location):
    cursor.execute("""
        INSERT INTO location_mapping (location_id, country_code, subdivision_code, name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id) DO UPDATE SET
            country_code = EXCLUDED.country_code,
            subdivision_code = EXCLUDED.subdivision_code,
            name = EXCLUDED.name;
    """, (location['location_id'], location['country_code'], location['subdivision_code'], location['name']))

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
                # Upsert location data
                upsert_location(cursor, location)
                print("Location data upserted successfully!")

                # Insert holiday data
                insert_holiday_data(cursor, location_id, holiday_data)
                print("Holiday data inserted successfully!")

        print("Location data updated and holiday data inserted successfully!")
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
