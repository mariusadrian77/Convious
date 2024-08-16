# Convious

To keep in mind that other considerations like the table or schema structures can be changed or altered. A bit too complex to consider all those changes in one ETL script. These considerations should be managed at a higher DB level, whether by adding multiple PROD-DEV environments that can be changed accordingly.

For most projects, separating the scripts is the better option, especially if:

You have a project that might scale or change over time.
You want to maintain clear separation of concerns between schema management and data population.
You plan to deploy in different environments where schema management is handled separately from data fetching and population.
