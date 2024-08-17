# Convious

To keep in mind that other considerations like the table or schema structures can be changed or altered. A bit too complex to consider all those changes in one ETL script. These considerations should be managed at a higher DB level, whether by adding multiple PROD-DEV environments that can be changed accordingly.

For most projects, separating the scripts is the better option, especially if:

You have a project that might scale or change over time.
You want to maintain clear separation of concerns between schema management and data population.
You plan to deploy in different environments where schema management is handled separately from data fetching and population.

Task 2

The objective is to design a system that can ingest, store and process multiple types of events from a backend responsible for checkout-related activities. Furthermore, the data needs to be analysis-ready while taking into account updates and modifications to stored past events.

First and foremost, data ingestion needs to be handled. There are two types of procedures that can be used for data ingestion, given the circumstances of the projects: streaming and batch ingestion. 

Given the data is used for "later analysis", I would opt for batch ingestions as it is less computationally demanding. In the case that data needs to be analyzed at the end of each week, in order to make assumptions for the following week, this data can be scheduled to run at the end of the week to aggregate and load data at intervals. This is not necessarily limited to a time frame, but it depends on the time scale at hand. This data will be used for analysis and subsequently to forecast future demand.
One of Convious' main features is the dynamic pricing of tickets. If needed, real-time streaming ingestion using a data pipeline such as Apache Kafka or AWS Kinesis can be used. This will allow for low-latency processing and real-time analytics. If the task at hand requires analyzing the flow of people coming today to an amusement park and raising the ticket prices along with it, streaming seems more appropriate.

Events could be stored in standardized JSON format. It's flexible, easy to use and future-proof to any changes such as new fields, or converting it to other formats. I would also run some additional data validation rules to maintain data quality. Personally, I have used Great Expectations, because it's the ample documentation makes it very easy to implement and customize in Python. Plus the reporting this tool provides is quite digestible for people without a technical background.

Data storage is one of the most important aspect of the system. I would consider two primary storage options: data lake and data warehouse. Data lakes are mostly used to handle large amounts of raw and unstructured data. This means that no schema needs to be predefined making it convenient to store any type of event data. That means that new fields can be added without breaking existing data. They are typically cheaper than data warehouses and more fit to large volumes of data over time. On the other hand, they don't handle complex queries very well and the lack of structure can make the data very hard to handle in terms of quality and consistency. Lastly, it requires an additional transformation step to make it suitable for analysis.

Data warehouses are designed for structured data with a predefined schema. This makes complex analytical queries easier due to indexing and partitioning. They are suited for aggregating data which is often required for BI tasks and reporting. Some of them come with built-in integrations for BI tools, making it easier to showcase to clients. In return, they tend to be more expensive and computationally demanding as the volume of data grows. Although, it is possible to change schemas in a data warehouse, it can be more challenging compared to a data lake. Lastly, they are not optimized for storing unstructured data, making them less flexible.

Nowadays, a combined approach is quite a common practice. I would store raw event data in a data lake, due to the vast amounts of data that might be coming in and its flexibility in adding new fields. This is ideal for data archiving and later transformations when needed for analysis. After storing raw even data, it can be processed periodically before loading it into a data warehouse. This data will have a proper structure, that allows it to be optimized for querying and analysis.

Now it has come to the schema design used in the data warehouse.
