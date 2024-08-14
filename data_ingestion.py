import psycopg2
from psycopg2 import sql
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Fetch the database URL from the .env file
DATABASE_URL = os.getenv("POSTGRESQL_KEY")

# Parse the URL to extract connection parameters
url = urlparse(DATABASE_URL)

conn_params = {
    'dbname': url.path[1:],    # Extracts the database name after '/'
    'user': url.username,       # Extracts the username
    'password': url.password,   # Extracts the password
    'host': url.hostname,       # Extracts the host
    'port': url.port            # Extracts the port
}

# Establish a connection to the database
conn = psycopg2.connect(**conn_params)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

def get_holidays_for_location(location_id, start_date, end_date):
    """
    Fetch holidays for a specific location within a given date range.

    :param location_id: UUID of the location.
    :param start_date: Start of the date range (YYYY-MM-DD).
    :param end_date: End of the date range (YYYY-MM-DD).
    :return: List of holidays in the specified range.
    """
    
    # SQL query to fetch holidays for the given location and date range
    query = """
    SELECT 
        h.date,
        h.name,
        h.observed,
        h.public
    FROM 
        holidays h
    JOIN 
        locations l 
    ON 
        h.location_id = l.location_id
    WHERE 
        l.location_id = %s
    AND 
        h.date BETWEEN %s AND %s;
    """

    cursor.execute(query, (location_id, start_date, end_date))
    
    holidays = cursor.fetchall()

    if holidays:
        print(f"Holidays for location_id {location_id} from {start_date} to {end_date}:")
        for holiday in holidays:
            print(f"Date: {holiday[0]}, Name: {holiday[1]}, Observed: {holiday[2]}, Public: {holiday[3]}")
    else:
        print(f"No holidays found for location_id {location_id} in the given date range.")
    
    return holidays

# Example usage
location_id = '9415913d-fffa-41f9-9323-6d62e6100a31'  # Replace with the desired location_id
start_date = '2023-01-01'  # Replace with the desired start date
end_date = '2023-12-31'  # Replace with the desired end date

# Fetch and print holidays
get_holidays_for_location(location_id, start_date, end_date)

# Close the cursor and connection
cursor.close()
conn.close()

print("Holiday data query complete!")
