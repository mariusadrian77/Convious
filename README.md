# Convious Data Engineering Task

## Introduction

This document outlines the considerations and design choices for building a system capable of ingesting, storing, and processing multiple types of events related to checkout activities. The goal is to ensure that the data is ready for analysis while accounting for updates and modifications to stored past events.

## Design Considerations

When working on a system like this, it's important to keep in mind that schema structures and table designs can be modified over time. These changes can be complex and should be managed at a higher database level. This can be done by having multiple PROD-DEV environments, which can be adjusted accordingly.

For most projects, separating scripts is recommended, especially if:

- The project might scale or change over time.
- There is a need to maintain a clear separation between schema management and data population.
- Deployment in different environments where schema management is handled separately from data ingestion is planned.

## Task 2: System Design

### Objective

Design a system that can ingest, store, and process multiple types of events from a backend responsible for checkout-related activities. The data needs to be analysis-ready and support updates and modifications to stored past events.

### Data Ingestion

There are two primary methods for data ingestion:

1. **Batch Ingestion:** Suitable for "later analysis" as it is less computationally demanding. For example, if data needs to be analyzed at the end of each week. In order to make assumptions for the following week, this data can be scheduled to run at the end of the week to aggregate and load data at intervals. This is not necessarily limited to a time frame, but it depends on the time scale at hand. This data will be used for analysis and subsequently to forecast future demand.
2. **Streaming Ingestion:** Real-time streaming ingestion using a data pipeline such as Apache Kafka or AWS Kinesis can be used. This will allow for low-latency processing and real-time analytics.

If the task at hand requires analyzing the flow of people coming today to an amusement park and raising the ticket prices along with it, streaming seems more appropriate. One of Convious' main features is the dynamic pricing of tickets, which can be done in real time.

I would opt for a batch ingestion method schedule at the end of each time period (day or week). The data will be analyzed to make predictions.
This can be done via an ETL tool (Prefect, Apache Airflow)

### Data Storage

Data storage is one of the most important aspects of the system. I would consider two primary storage options: data lake and data warehouse

1. **Data Lake:**
Mostly used to handle large amounts of raw and unstructured data. This means that no schema needs to be predefined making it convenient to store any type of event data. That means that new fields can be added without breaking existing data. They are typically cheaper than data warehouses and more fit to large volumes of data over time. On the other hand, they don't handle complex queries very well and the lack of structure can make the data very hard to handle in terms of quality and consistency. Lastly, it requires an additional transformation step to make it suitable for analysis.
   
3. **Data Warehouse:**
Designed for structured data with a predefined schema. This makes complex analytical queries easier due to indexing, partitioning and the capability of creating materialized views. They are suited for aggregating data which is often required for BI tasks and reporting. Some of them come with built-in integrations for BI tools, making it easier to showcase to clients. In return, they tend to be more expensive and computationally demanding as the volume of data grows. Although, it is possible to change schemas in a data warehouse, it can be more challenging compared to a data lake. Lastly, they are not optimized for storing unstructured data, making them less flexible.

4. **Combined Approach:**
Nowadays, a combined approach is quite a common practice. I would store raw event data in a data lake, due to the vast amounts of data that might be coming in and its flexibility in adding new fields. This is ideal for data archiving and later transformations when needed for analysis. After storing raw event data, it can be processed periodically before loading it into a data warehouse. This data will have a proper structure, that allows it to be optimized for querying and analysis. 

I have been using Snowflake for the past year or so. And I consider it's suitable for the task at hand. It allows the user to scale storage and resources independently, it has cross-platform compatibility and it is optimized for both real-time and batch processing. In a larger team, it allows accounts to share data seamlessly without moving any data. Given the multitude of clients that Convinus has, different clients might use different cloud environments and Snowflake is quite convenient in this case. There are certainly other alternatives like Amazon Redshift is the AWS ecosystem is widely used.

### Schema Design

Events can be quite complex, but without knowing the actual data structure of the project, I would go for the less complex star schema because it is well-suited for analytical tasks and simplifies querying and reporting. It is organized around a central fact table (transactional data), surrounded by multiple dimensional ones (descriptive attributes). The structure is easy to use for BI tools which makes it very digestible. This is due to the fact that the data is typically denormalized, where everything is stored in one place (the fact table). This eliminates the need for complex joins across multiple tables because those tables are interconnected. Furthermore, the star schema is efficient for handling large datasets because the dimension tables are generally smaller and more manageable, without changing too frequently. 

An example of star schema design for our ticket system:

### Fact Table: `events`

The `events` table stores all event-related data.

| Column Name     | Data Type     | Description                                             |
|-----------------|---------------|---------------------------------------------------------|
| event_id        | UUID          | Unique identifier for each event                        |
| event_type      | VARCHAR       | Type of event (e.g., "ArticleAddedToCart")              |
| event_timestamp | TIMESTAMP     | Timestamp of when the event occurred                    |
| user_id         | UUID          | ID of the user associated with the event                |
| related_id      | UUID          | ID of the related entity (e.g., cart_id, ticket_id)     |
| version         | INT           | Version number for event versioning                     |
| metadata        | JSONB         | JSON data with additional event-specific metadata       |

#### Example Data for `events` Table:

| event_id                              | event_type          | event_timestamp     | user_id                              | related_id                           | version | metadata                                                                                  |
|---------------------------------------|---------------------|---------------------|--------------------------------------|--------------------------------------|---------|-------------------------------------------------------------------------------------------|
| 1a2b3c4d-1234-5678-9abc-123456789abc   | ArticleAddedToCart  | 2024-08-17 10:00:00 | 123e4567-e89b-12d3-a456-426614174000 | 098f6bcd-4621-3373-8ade-4e832627b4f6 | 1       | {"article_id": "98765", "article_price": 29.99, "currency": "USD"}                        |
| 2b3c4d5e-2234-5678-9def-223456789def   | PaymentSuccessful   | 2024-08-17 10:05:00 | 123e4567-e89b-12d3-a456-426614174000 | 678f6bcd-5621-3373-8bde-4e832627b4f7 | 1       | {"total_amount": 59.99, "currency": "USD", "payment_method": "CreditCard"}                |
| 3c4d5e6f-3234-5678-9ghi-323456789ghi   | BarcodeScanned      | 2024-08-17 10:10:00 | 123e4567-e89b-12d3-a456-426614174000 | 543f6bcd-6621-3373-8cde-4e832627b4f8 | 2       | {"barcode_type": "QR", "scanner_location": "Entrance A"}                                  |

### Dimension Tables

#### `users` Table

| Column Name  | Data Type | Description                |
|--------------|-----------|----------------------------|
| user_id      | UUID      | Unique identifier for users |
| name         | VARCHAR   | Name of the user            |
| email        | VARCHAR   | Email address of the user   |
| country      | VARCHAR   | Country of the user         |

#### Example Data for `users` Table:

| user_id                               | name      | email                     | country     |
|---------------------------------------|-----------|----------------------------|-------------|
| 123e4567-e89b-12d3-a456-426614174000   | John Doe  | john.doe@example.com       | USA         |
| 789e4567-e89b-12d3-a456-426614174001   | Jane Doe  | jane.doe@example.com       | Canada      |

#### `tickets` Table

| Column Name  | Data Type | Description                     |
|--------------|-----------|---------------------------------|
| ticket_id    | UUID      | Unique identifier for tickets   |
| event_name   | VARCHAR   | Name of the event associated with the ticket |
| ticket_price | DECIMAL   | Price of the ticket             |

#### Example Data for `tickets` Table:

| ticket_id                             | event_name         | ticket_price |
|---------------------------------------|--------------------|--------------|
| 543f6bcd-6621-3373-8cde-4e832627b4f8  | Music Concert      | 99.99        |
| 678f6bcd-5621-3373-8bde-4e832627b4f7  | Theater Play       | 49.99        |

#### `articles` Table

| Column Name  | Data Type | Description                   |
|--------------|-----------|-------------------------------|
| article_id   | UUID      | Unique identifier for articles|
| article_name | VARCHAR   | Name of the article            |
| price        | DECIMAL   | Price of the article           |
| currency     | VARCHAR   | Currency of the price          |

#### Example Data for `articles` Table:

| article_id                            | article_name       | price | currency |
|---------------------------------------|--------------------|-------|----------|
| 98765                                 | Winter Jacket      | 29.99 | USD      |
| 12345                                 | Running Shoes      | 59.99 | USD      |

## Event Versioning and Handling Updates

Handling updates to past events adds another level of complexity to the system. So far, I have combated this through two levels of security. 

1. **Data Lakes:** Storing large amounts of raw data, which allows making changes to the data easier. 
2. **Event Versioning:** Adding a `version` and `metadata` field to the fact tables to track changes and make version control easier to handle.

For example, you can use an upsert operation (insert or update) based on the event ID and version to ensure past events are properly represented in the warehouse.

### Reprocessing Pipeline

I would implement a reprocessing pipeline to check if new data has been added to past events. This can be triggered whenever there is a significant change or new fields are added (e.g. a new field "barcode_type" is added, then reprocess historical BarcodeScanned events to update the warehouse; tracking and logging all changes is important to ensure data integrity for an audit trail).

## Additional Considerations

Further considerations can be put into the system:

- **Data Catalog:** Documenting data for better understanding.
- **Role-Based Access Control:** Ensuring only authorized users can access specific data.
- **Schema Evolution:** Adapting to changing data requirements over time.
