import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

def create_tables_and_indexes():
    # Load environment variables from the .env file
    load_dotenv()

    # Fetch the database URL from the .env file
    POSTGRESQL_URL = os.getenv("POSTGRESQL_KEY")

    # Parse the URL to extract connection parameters
    url = urlparse(POSTGRESQL_URL)

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

    # Example: Create a table for location data (adjust column names and types as per your needs)
    cursor.execute("""               
        CREATE TABLE IF NOT EXISTS location_mapping (
            location_id UUID PRIMARY KEY,
            country_code VARCHAR(2) NOT NULL,
            subdivision_code VARCHAR(10),
            name VARCHAR(255) NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE, -- Column to indicate if the location is active or inactive
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Column to track when the record was last updated
        );
    """)

    # Example: Create a table for holiday data (adjust column names and types as per your needs)
    cursor.execute("""               
        CREATE TABLE IF NOT EXISTS holidays (
            location_id UUID NOT NULL REFERENCES location_mapping(location_id),
            version INT NOT NULL, -- Version number to track updates
            date DATE NOT NULL,
            name VARCHAR(255),
            observed DATE,
            public BOOLEAN,
            current_version BOOLEAN NOT NULL DEFAULT TRUE, -- Flag to indicate the latest version
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (location_id, version, date)
        );
    """)


    # Commit the transaction to save changes
    conn.commit()

    print("Tables created successfully!")

    # Indexing queries to improve performance
    # Indexes for the holidays table
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_country_subdivision 
        ON location_mapping(country_code, subdivision_code);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_location_date 
        ON holidays(location_id, date);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_current_version 
        ON holidays(location_id, current_version);
    """)

    # Commit the transaction to save changes
    conn.commit()

    print("Tables indexed successfully!")

    # Uncomment the lines below to drop the tables if necessary
    # cursor.execute("DROP TABLE IF EXISTS holidays;")
    # cursor.execute("DROP TABLE IF EXISTS location_mapping;") 

    # # Commit the changes
    # conn.commit()

    # print("Tables deleted successfully!")

    # Close the cursor and connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables_and_indexes()
