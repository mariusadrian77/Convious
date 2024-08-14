import requests
import json
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Fetch the database URL from the .env file
DATABASE_URL = os.getenv("HOLIDAY_API_KEY")

# Define your API key
API_KEY = '553d0f97-886b-4d71-ab8b-d06a763cea6c'

# List of locations with their corresponding country codes and subdivision codes
locations = [
    {"location_id": "9415913d-fffa-41f9-9323-6d62e6100a31", "country_code": "NL", "subdivision_code": "NL-FL"},
    # {"location_id": "432d10d2-1a7c-4bbd-abd2-85075fb19c71", "country_code": "NL", "subdivision_code": "NL-GR"},
    # {"location_id": "a2d0d6fd-3a18-4f58-ac36-d2e56bf71a46", "country_code": "GB", "subdivision_code": "GB-NSM"},
    # {"location_id": "ab5df8c0-dfe7-4ca3-a9e4-c77f93e551a7", "country_code": "NL", "subdivision_code": "NL-ZH"},
    # {"location_id": "fdbf55b4-1b97-43a8-a096-a71d0b9d6940", "country_code": "GB", "subdivision_code": "GB-WLL"},
    # {"location_id": "d9a11093-b1a4-4c1a-9e2a-7cc951b55a32", "country_code": "NL", "subdivision_code": "NL-DR"},
    # {"location_id": "4f2c9d63-73b3-40f4-892d-136599854b87", "country_code": "NL", "subdivision_code": "NL-FR"},
    # {"location_id": "423ff765-83ac-472c-9ef7-b3a592696711", "country_code": "NL", "subdivision_code": "NL-UT"},
    # {"location_id": "5770db11-e7bf-4044-b54c-d49f69e947ec", "country_code": "GB", "subdivision_code": "GB-SOM"},
    # {"location_id": "d70aed5a-3960-44fa-9c08-f725c2b03ce8", "country_code": "NL", "subdivision_code": "NL-NH"},
    # {"location_id": "e826584c-c32b-4ca1-835f-b7d7416f2958", "country_code": "NL", "subdivision_code": "NL-LI"},
    # {"location_id": "e40a0514-03e7-4d28-b7be-38c18a5ae73c", "country_code": "GB", "subdivision_code": "GB-WRT"},
    # {"location_id": "4c8eb5ba-0140-4b48-924b-899112abe562", "country_code": "NL", "subdivision_code": "NL-GE"},
]

# Define the base URL for the API
url = "https://holidayapi.com/v1/holidays"

# Function to fetch holiday data for a specific location
def fetch_holiday_data(country_code, year, subdivision_code=None):
    params = {
        'key': API_KEY,
        'country': country_code,
        'year': year,
        'pretty': 'true'  # Optional: format the response for readability
    }
    if subdivision_code:
        params['subdivision'] = subdivision_code
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {country_code}-{subdivision_code}. Status code: {response.status_code}")
        return None

# Fetch and print holiday data for all locations
year = 2024  # Specify the year for which you want to fetch holiday data

all_holiday_data = {}

for location in locations:
    country_code = location["country_code"]
    subdivision_code = location["subdivision_code"]
    location_id = location["location_id"]
    
    print(f"Fetching holiday data for {country_code}-{subdivision_code}...")
    # Free version ONLY allows to fetch data for the previous year
    data = fetch_holiday_data(country_code, 2023, subdivision_code)
    
    if data:
        all_holiday_data[location_id] = data
        # Optional: Print data for each location
        print(json.dumps(data, indent=4))

# Optionally save the fetched data to a file for later use
with open('holiday_data.json', 'w') as f:
    json.dump(all_holiday_data, f, indent=4)

print("Holiday data fetching complete!")
