import psycopg2
from dotenv import load_dotenv
import os
from urllib.parse import urlparse

# Load environment variables from the .env file
load_dotenv()

# Fetch the database URL from the .env file
POSTGRESQL_URL = os.getenv("POSTGRESQL_KEY")

# Parse the database URL to extract connection parameters
postgressql_url = urlparse(POSTGRESQL_URL)
# Extract the database credentials
conn_params = {
    'dbname': postgressql_url.path[1:],    
    'user': postgressql_url.username,       
    'password': postgressql_url.password,   
    'host': postgressql_url.hostname,       
    'port': postgressql_url.port          
}

def import_holidays_for_location(location_id, start_date, end_date):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Define the SQL query
        query = """
        SELECT h.location_id, h.date, h.name, h.observed, h.public, h.version
        FROM holidays h
        JOIN location_mapping lm ON h.location_id = lm.location_id
        WHERE lm.location_id = %s
          AND h.date BETWEEN %s AND %s
          AND h.current_version = TRUE
          AND lm.active = TRUE;
        """

        # Execute the query
        cursor.execute(query, (location_id, start_date, end_date))

        # Fetch and return the results
        holidays = cursor.fetchall()
        return holidays

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Example usage
if __name__ == "__main__":
    location_id = '9415913d-fffa-41f9-9323-6d62e6100a31'
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    
    holidays = import_holidays_for_location(location_id, start_date, end_date)
    if holidays:
        for holiday in holidays:
            print(holiday)
    else:
        print("No holidays found.")
