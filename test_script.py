import pytest
import requests
from unittest.mock import Mock
from ETL_V2 import fetch_holiday_data, upsert_location, insert_holiday_data, deactivate_location, deactivate_removed_locations, main
from holiday_query import import_holidays_for_location
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os
from urllib.parse import urlparse

# Load environment variables from the .env file
load_dotenv()

# Fetch the database URL from the .env file
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

# Connect to the PostgreSQL database
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

def test_insert_new_location_and_holidays():
    location = {
        "location_id": "new-location-id",
        "country_code": "NL",
        "subdivision_code": "NL-ZH",
        "name": "Test Location"
    }
    # Insert holidays
    holiday_data = {
        "holidays": [
            {
                "date": "2023-12-25",
                "name": "Christmas Day",
                "observed": "2023-12-25",
                "public": True
            }
        ]
    }
    # Upsert location and insert holidays
    upsert_location(cursor, location)
    insert_holiday_data(cursor, location['location_id'], holiday_data)

    # Query the database to ensure the data was inserted correctly
    cursor.execute("""
        SELECT * FROM location_mapping WHERE location_id = %s
    """, (location['location_id'],))
    location_result = cursor.fetchone()

    cursor.execute("""
        SELECT * FROM holidays WHERE location_id = %s AND date = %s
    """, (location['location_id'], '2023-12-25'))
    holiday_result = cursor.fetchone()

    assert location_result['version'] == 1
    assert location_result['active'] == True
    assert holiday_result['version'] == 1
    assert holiday_result['current_version'] == True

def test_update_existing_location():
    location = {
        "location_id": "existing-location-id",
        "country_code": "NL",
        "subdivision_code": "NL-FL",
        "name": "Updated Location"
    }
    holiday_data = {
        "holidays": [
            {
                "date": "2023-12-31",
                "name": "New Year's Eve",
                "observed": "2023-12-31",
                "public": True
            }
        ]
    }
    # Upsert location and insert holidays
    upsert_location(cursor, location)
    insert_holiday_data(cursor, location['location_id'], holiday_data)

    # Query to check the new version is created
    cursor.execute("""
        SELECT * FROM location_mapping WHERE location_id = %s ORDER BY version DESC LIMIT 1
    """, (location['location_id'],))
    location_result = cursor.fetchone()

    cursor.execute("""
        SELECT * FROM holidays WHERE location_id = %s AND date = %s ORDER BY version DESC LIMIT 1
    """, (location['location_id'], '2023-12-31'))
    holiday_result = cursor.fetchone()

    assert location_result['version'] > 1
    assert location_result['current_version'] == True
    assert holiday_result['version'] > 1
    assert holiday_result['current_version'] == True

    # Verify the previous version is marked as not current
    cursor.execute("""
        SELECT * FROM location_mapping WHERE location_id = %s AND version < %s
    """, (location['location_id'], location_result['version']))
    old_location_result = cursor.fetchone()

    assert old_location_result['current_version'] == False

def test_soft_delete_location():
    location_id = "location-to-delete"
    
    # Simulate soft deletion
    cursor.execute("""
        UPDATE location_mapping SET active = FALSE WHERE location_id = %s
    """, (location_id,))
    conn.commit()

    # Query to check if the location is marked as inactive
    cursor.execute("""
        SELECT * FROM location_mapping WHERE location_id = %s
    """, (location_id,))
    location_result = cursor.fetchone()

    assert location_result['active'] == False

    # Check if the holidays are marked as not current
    cursor.execute("""
        SELECT * FROM holidays WHERE location_id = %s
    """, (location_id,))
    holidays_result = cursor.fetchall()

    for holiday in holidays_result:
        assert holiday['current_version'] == False


def test_fetch_holidays():
    location_id = "9415913d-fffa-41f9-9323-6d62e6100a31"
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    holidays = import_holidays_for_location(location_id, start_date, end_date)

    assert holidays is not None
    assert len(holidays) > 0

    for holiday in holidays:
        assert holiday['date'] >= start_date and holiday['date'] <= end_date
        assert holiday['location_id'] == location_id

def test_duplicate_key_handling():
    location = {
        "location_id": "duplicate-location-id",
        "country_code": "NL",
        "subdivision_code": "NL-FL",
        "name": "Flevoland"
    }

    try:
        upsert_location(cursor, location)
        conn.commit()
    except Exception as e:
        assert "duplicate key" in str(e).lower()

    # Ensure no data was corrupted or incorrectly inserted
    cursor.execute("""
        SELECT COUNT(*) FROM location_mapping WHERE location_id = %s
    """, (location['location_id'],))
    count_result = cursor.fetchone()

    assert count_result[0] == 1
