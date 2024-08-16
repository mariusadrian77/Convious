import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

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

# Example: Create a table for holiday data (adjust column names and types as per your needs)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS holidays (
        id SERIAL PRIMARY KEY,
        location_id UUID NOT NULL,
        date DATE NOT NULL,
        name VARCHAR(255) NOT NULL,
        observed DATE NOT NULL,
        public BOOLEAN NOT NULL
    );
""")

# Example: Create a table for location data (adjust column names and types as per your needs)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS locations (
        location_id UUID PRIMARY KEY,
        country_code VARCHAR(3) NOT NULL,
        subdivision_code VARCHAR(10) NOT NULL,
        name VARCHAR(255) NOT NULL
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS location_mapping (
        location_id UUID PRIMARY KEY,
        country_code VARCHAR(3) NOT NULL,
        subdivision_code VARCHAR(10),
        name VARCHAR(255),
        active BOOLEAN DEFAULT TRUE 
    );
""")

# Commit the transaction to save changes
conn.commit()

print("Tables created successfully!")

# Indexing queries to improve performance
cursor.execute("CREATE INDEX IF NOT EXISTS idx_location_id ON holidays(location_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON holidays(date);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_country_subdivision ON locations(country_code, subdivision_code);")

print("Tables indexed successfully!")

# Commit the transaction to save changes
conn.commit()

# cursor.execute("DROP TABLE IF EXISTS holidays;")
# cursor.execute("DROP TABLE IF EXISTS location_mapping;")
# cursor.execute("DROP TABLE IF EXISTS locations;")  

# # Commit the changes
# conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()