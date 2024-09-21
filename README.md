### Objective

Design a service which would give the holiday dates for a specific location and requested time range. The information is about holidays in different locations, given the list below:

| location_id                               | country_code | subdivision_code |
|-------------------------------------------|--------------|------------------|
| 9415913d-fffa-41f9-9323-6d62e6100a31       | NL           | NL-FL            |
| 432d10d2-1a7c-4bbd-abd2-85075fb19c71       | NL           | NL-GR            |
| a2d0d6fd-3a18-4f58-ac36-d2e56bf71a46       | GB           | GB-NSM           |
| ab5df8c0-dfe7-4ca3-a9e4-c77f93e551a7       | NL           | NL-ZH            |
| fdbf55b4-1b97-43a8-a096-a71d0b9d6940       | GB           | GB-WLL           |
| d9a11093-b1a4-4c1a-9e2a-7cc951b55a32       | NL           | NL-DR            |
| 4f2c9d63-73b3-40f4-892d-136599854b87       | NL           | NL-FR            |
| 423ff765-83ac-472c-9ef7-b3a592696711       | NL           | NL-UT            |
| 5770db11-e7bf-4044-b54c-d49f69e947ec       | GB           | GB-SOM           |
| d70aed5a-3960-44fa-9c08-f725c2b03ce8       | NL           | NL-NH            |
| e826584c-c32b-4ca1-835f-b7d7416f2958       | NL           | NL-LI            |
| e40a0514-03e7-4d28-b7be-38c18a5ae73c       | GB           | GB-WRT           |
| 4c8eb5ba-0140-4b48-924b-899112abe562       | NL           | NL-GE            |

## System Design

I designed three Python scripts that work together to manage holiday data for various locations using PostgreSQL. My platform of choice for the PostgreSQL database is Railway, because it offers a free plan that is easy to deploy and scale and allows to create and manage PostgreSQL databases. Furthermore, everything including running the SQL queries is managed through Python. The scripts handle the creation of database tables, fetching and inserting holiday data from an external API, and querying the holiday data. Below is a detailed guide on setting up and using these scripts.

## Scripts Overview

1. **postregressql_db_creation.py**
   - This script is responsible for creating the necessary tables (`location_mapping` and `holidays`) and indexes in the PostgreSQL database.

   **Tables:**
   - `location_mapping`: Stores location information (country code, subdivision, etc.).
   - `holidays`: Stores holiday data, including the holiday name, date, and versioning.

   **Indexes:**
   - `idx_country_subdivision`: Speeds up queries on `country_code` and `subdivision_code`.
   - `idx_location_date`: Improves the performance of queries filtering by `location_id` and `date`.
   - `idx_current_version`: Ensures efficient retrieval of the latest holiday data versions.

2. **ETL.py**
   - This script fetches holiday data from the HolidayAPI for predefined locations and inserts or updates the data in the PostgreSQL database.

   **Functions:**
   - `fetch_holiday_data()`: Fetches holiday data for a specific location and year.
   - `upsert_location()`: Inserts or updates location data in `location_mapping`.
   - `insert_holiday_data()`: Inserts holiday data with versioning, ensuring the latest data is flagged as `current_version`.
   - `deactivate_location()`: Marks locations that are no longer active.
   - `deactivate_removed_locations()`: Deactivates locations that are no longer fetched in the current run.

3. **holiday_query.py**
   - This script allows you to query the holiday data stored in the PostgreSQL database for a given `location_id` and date range.

   **Functions:**
   - `import_holidays_for_location()`: Queries the database for holidays within a specified date range for a given location, returning the results.

## Design Considerations

When working on a system like this, it's important to keep in mind that schema structures and table designs can be modified over time. These changes can be complex and should be managed at a higher database level. This can be done by having multiple PROD-DEV environments, which can be adjusted accordingly.

For most projects, separating scripts is recommended, especially if:

- The project might scale or change over time.
- There is a need to maintain a clear separation between schema management and data population.
- Deployment in different environments where schema management is handled separately from data ingestion is planned.

The complexity of this task lies in the addition or removal of locations at any time. This also needs to be scalable and perform relatively well, without changing the query search to accommodate for unforeseen situations. Keeping this in mind, I decided to keep the database schema relatively simple by having only two tables: `holidays` and `location_mapping` (mentioned in the Script Overview).

### Purpose of Versioning

A crucial aspect of this process is versioning, which is used to manage and track changes to the holiday data over time. By storing multiple versions of the holiday data, we ensure that no data is overwritten or lost when updates are made. This creates historical records (archive), whether the data was removed or altered.

### How Versioning is Implemented

- **Version Column**: In the `holidays` table, there is a `version` column that tracks the version number of each record. When new holiday data is fetched for a location, the script determines the next version number by querying the maximum version number currently stored for that location and then incrementing it by one.

- **Current Version Flag**: The `current_version` boolean column indicates which version of the holiday data is the most recent. Before inserting new holiday data, the script sets the `current_version` flag to `FALSE` for the current latest version, ensuring that only the newly inserted records have `current_version` set to `TRUE`.

- **Deactivation of Old Versions**: By updating the `current_version` flag, the script ensures that only the most recent data is considered when querying for holidays. This flag allows the system to efficiently filter out older versions without physically deleting them, thus retaining the full history of changes.

### Benefits of Versioning for Task Requirements

- **Querying Current Data**: By using the `current_version` flag, the script ensures that the query will only return the latest data, providing accurate and up-to-date information.
- **Data Consistency**: In case of multiple updates to the holiday data, versioning ensures that all changes are logged and tracked. This means that even if an update occurs after the initial data fetch, the system can still provide consistent and reliable information. In such cases where the location is removed, then the value is not in use anymore. This also accounts for changes in the location, such as the naming or codes.
- **Historical Analysis**: Storing multiple versions allows to analyze holidays data changes over time. This can be helpful for other purposes (audit) or for understanding the impact of changes over time, even if some of the data becomes obsolete.

### Trade-offs of Versioning for Task Requirements

- **Query Performance**: Although the `current_version` index helps with query performance, the presence of multiple versions can still complicate query optimization. If the database grows large due to many versions, queries might slow down, especially those that do not use the `current_version` flag or those that require access to historical data. Additionally, maintaining the `current_version` index may add a small performance overhead during data insertion.

- **Scalability**: As the dataset grows, the versioning system might become a bottleneck. Large-scale deployments might require more sophisticated solutions, such as partitioning data by location or time period or using a more advanced version control system to avoid performance degradation. Lastly, the holiday table might become overcrowded.

- **Maintenance and Cleanup**: Over time, old versions may accumulate, leading to a bloated database. Running periodic cleanup processes may become necessary to manage database size.

### Example Workflow

When the script runs for the first time, it fetches holiday data for a given year and five locations, assigns version 1 to all records, and sets `current_version` to `TRUE`. The next time the script runs, it increments the version number (version 2), sets `current_version` to `FALSE` for the old records, and inserts the new data with `current_version` set to `TRUE`. The third time the script runs, only four of the initial five locations are fetched, because one of them got deleted. The version number is incremented (version 3) and the `current_version` is set to `FALSE` for all records for four of the five locations, whereas the fifth one still has its `current_version` set to `TRUE`. In addition, the active flag in the holiday_mapping table becomes `FALSE` for the fifth location.

Lastly, when querying the holidays, the script will automatically filter out the old data by selecting only records where `current_version` is `TRUE`, ensuring the user always sees the latest version.
