import unittest
from unittest.mock import patch, MagicMock
import requests
import psycopg2
from ETL_V2 import fetch_holiday_data, upsert_location, insert_holiday_data, deactivate_location, deactivate_removed_locations

class TestHolidayDataFunctions(unittest.TestCase):

    @patch('ETL_V2.requests.get')
    def test_fetch_holiday_data_success(self, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'holidays': [
                {'date': '2023-01-01', 'name': 'New Year', 'observed': '2023-01-01', 'public': True}
            ]
        }
        mock_get.return_value = mock_response
        
        result = fetch_holiday_data('NL', 2023, 'NL-FL')
        self.assertIsNotNone(result)
        self.assertEqual(len(result['holidays']), 1)
        self.assertEqual(result['holidays'][0]['name'], 'New Year')

    @patch('ETL_V2.requests.get')
    def test_fetch_holiday_data_failure(self, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = fetch_holiday_data('NL', 2023, 'NL-FL')
        self.assertIsNone(result)

    @patch('ETL_V2.psycopg2.connect')
    def test_upsert_location(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        location = {
            'location_id': '9415913d-fffa-41f9-9323-6d62e6100a31',
            'country_code': 'NL',
            'subdivision_code': 'NL-FL',
            'name': 'Flevoland'
        }
        
        upsert_location(mock_cursor, location)
        mock_cursor.execute.assert_called_once()

    @patch('ETL_V2.psycopg2.connect')
    def test_insert_holiday_data(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        location_id = '9415913d-fffa-41f9-9323-6d62e6100a31'
        holiday_data = {
            'holidays': [
                {'date': '2023-01-01', 'name': 'New Year', 'observed': '2023-01-01', 'public': True}
            ]
        }
        version = 1
        
        insert_holiday_data(mock_cursor, location_id, holiday_data, version)
        self.assertEqual(mock_cursor.execute.call_count, 2)

    @patch('ETL_V2.psycopg2.connect')
    def test_deactivate_location(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        location_id = '9415913d-fffa-41f9-9323-6d62e6100a31'
        deactivate_location(mock_cursor, location_id)
        mock_cursor.execute.assert_called_once()

    @patch('ETL_V2.psycopg2.connect')
    def test_deactivate_removed_locations(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        current_location_ids = ['9415913d-fffa-41f9-9323-6d62e6100a31']
        deactivate_removed_locations(mock_cursor, current_location_ids)
        mock_cursor.execute.assert_called_once()

if __name__ == '__main__':
    unittest.main()
