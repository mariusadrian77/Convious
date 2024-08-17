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

# Example: Create a table for location data (adjust column names and types as per your needs)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS location_mapping (
        location_id UUID PRIMARY KEY,
        country_code VARCHAR(2) NOT NULL,
        subdivision_code VARCHAR(10) NOT NULL,
        name VARCHAR(255) NOT NULL,
        version INT NOT NULL,
        UNIQUE (location_id, version)
    );
""")

# Example: Create a table for holiday data (adjust column names and types as per your needs)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS holidays (
        holiday_id SERIAL PRIMARY KEY,      
        location_id UUID NOT NULL,          
        version INT NOT NULL,               
        date DATE NOT NULL,                 
        name VARCHAR(255) NOT NULL,         
        observed DATE,                      
        public BOOLEAN NOT NULL,            
        FOREIGN KEY (location_id, version) 
            REFERENCES location_mapping(location_id, version) 
            ON DELETE CASCADE                
    );
""")


# Commit the transaction to save changes
conn.commit()

print("Tables created successfully!")

# Indexing queries to improve performance
# Indexes for the holidays table
cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_location_version 
    ON holidays(location_id, version);
""")
cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_date 
    ON holidays(date);
""")
# Indexes for the location_mapping table
cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_country_subdivision 
    ON location_mapping(country_code, subdivision_code);
""")


# Commit the transaction to save changes
conn.commit()

print("Tables indexed successfully!")

# cursor.execute("DROP TABLE IF EXISTS holidays;")
# cursor.execute("DROP TABLE IF EXISTS location_mapping;")
# cursor.execute("DROP TABLE IF EXISTS locations;")  

# # Commit the changes
# conn.commit()

# print("Tables deleted successfully!")

# Close the cursor and connection
cursor.close()
conn.close()