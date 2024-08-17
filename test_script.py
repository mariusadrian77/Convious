import pytest
import requests
from unittest.mock import Mock
from ETL import fetch_holiday_data, upsert_location, insert_holiday_data, main
import psycopg2
from psycopg2 import pool

# Mock environment variables
import os
from dotenv import load_dotenv

def test_fetch_holiday_data_success(mocker):
    mock_response = {
        'holidays': [{'date': '2023-01-01', 'name': 'New Year', 'observed': '2023-01-01', 'public': True}]
    }
    mock_get = mocker.patch('requests.get', return_value=Mock(status_code=200, json=lambda: mock_response))
    result = fetch_holiday_data('NL', 2023, 'NL-FL')
    assert result == mock_response
    mock_get.assert_called_once_with("https://holidayapi.com/v1/holidays", params={
        'key': os.getenv("HOLIDAY_API_KEY"),
        'country': 'NL',
        'year': 2023,
        'pretty': 'true',
        'subdivision': 'NL-FL'
    })

def test_fetch_holiday_data_failure(mocker):
    mock_get = mocker.patch('requests.get', return_value=Mock(status_code=500))
    result = fetch_holiday_data('NL', 2023, 'NL-FL')
    assert result is None
    mock_get.assert_called_once_with("https://holidayapi.com/v1/holidays", params={
        'key': os.getenv("HOLIDAY_API_KEY"),
        'country': 'NL',
        'year': 2023,
        'pretty': 'true',
        'subdivision': 'NL-FL'
    })

def test_upsert_location(mocker):
    cursor = mocker.Mock()
    location = {
        'location_id': 'test-id',
        'country_code': 'NL',
        'subdivision_code': 'NL-FL',
        'name': 'Flevoland'
    }
    upsert_location(cursor, location)
    cursor.execute.assert_called_once_with("""
        INSERT INTO location_mapping (location_id, country_code, subdivision_code, name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id) DO UPDATE SET
            country_code = EXCLUDED.country_code,
            subdivision_code = EXCLUDED.subdivision_code,
            name = EXCLUDED.name;
    """, ('test-id', 'NL', 'NL-FL', 'Flevoland'))

def test_insert_holiday_data(mocker):
    cursor = mocker.Mock()
    location_id = 'test-id'
    holiday_data = {
        'holidays': [
            {'date': '2023-01-01', 'name': 'New Year', 'observed': '2023-01-01', 'public': True}
        ]
    }
    insert_holiday_data(cursor, location_id, holiday_data)
    cursor.execute.assert_called_once_with("""
        INSERT INTO holidays (location_id, date, name, observed, public)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        'test-id',
        '2023-01-01',
        'New Year',
        '2023-01-01',
        True
    ))

def test_full_execution(mocker):
    # Mock environment variables
    mocker.patch.dict(os.environ, {
        'HOLIDAY_API_KEY': 'test_key',
        'POSTGRESQL_KEY': 'postgresql://user:password@localhost:5432/testdb'
    })

    # Mocking API response
    mock_response = {
        'holidays': [{'date': '2023-01-01', 'name': 'New Year', 'observed': '2023-01-01', 'public': True}]
    }
    mock_get = mocker.patch('requests.get', return_value=Mock(status_code=200, json=lambda: mock_response))

    # Mocking database connection and cursor
    mock_pool = mocker.patch('psycopg2.pool.SimpleConnectionPool', return_value=Mock())
    mock_conn = mock_pool.return_value.getconn.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Running the main function
    main()

    # Assert API call
    mock_get.assert_called_once()

    # Assert database operations
    mock_cursor.execute.assert_any_call("""
        INSERT INTO location_mapping (location_id, country_code, subdivision_code, name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id) DO UPDATE SET
            country_code = EXCLUDED.country_code,
            subdivision_code = EXCLUDED.subdivision_code,
            name = EXCLUDED.name;
    """, ('9415913d-fffa-41f9-9323-6d62e6100a31', 'NL', 'NL-FL', 'Flevoland'))

    mock_cursor.execute.assert_any_call("""
        INSERT INTO holidays (location_id, date, name, observed, public)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        '9415913d-fffa-41f9-9323-6d62e6100a31',
        '2023-01-01',
        'New Year',
        '2023-01-01',
        True
    ))

def test_main_database_failure(mocker):
    # Mock environment variables
    mocker.patch.dict(os.environ, {
        'HOLIDAY_API_KEY': 'test_key',
        'POSTGRESQL_KEY': 'postgresql://user:password@localhost:5432/testdb'
    })

    # Simulate a database connection error
    mocker.patch('psycopg2.pool.SimpleConnectionPool', side_effect=Exception("Database connection error"))
    with pytest.raises(Exception, match="Database connection error"):
        main()
