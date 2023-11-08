

# Change Data Capture (Postgres -> Debezium/Kafka -> Clickhouse -> Superset)

## Table of Contents

-  [Table of Contents](#table-of-contents)

-  [Introduction](#introduction)

-  [How to Start](#how-to-start)
	1. [Project Setup](#project-setup)
	2. [OLTP Database: Postgres](#oltp-database-postgres)
	3. [OLAP Clickhouse Database: Tables Creations and Copy Postgres Records](#olap-clickhouse-database-tables-creations-and-copy-postgres-records)
	4. [CDC: Debezium/Kafka](#cdc-with-debezium-and-kafka)
	5. [Databases Sync: Connect Clickhouse Tables to Kafka Topics](#databases-sync-connect-clickhouse-tables-to-kafka-topics)
	6. [Data Visualization: Superset](#data-visualization-superset)
	
- [Scripts List](#scripts-list)
- [Data Dictionary: Clickhouse OLAP](#data-dictionary-clickhouse-olap)
- [OLAP Query Examples](#olap-query-examples)


## Introduction

This data integration project harnesses the power of **Debezium**, **Kafka**, and **ClickHouse** to create a seamless flow of data changes from a **PostgreSQL** database to a ClickHouse **Data Warehouse**. The core objective of this project is to establish a streamlined and real-time data replication process. This empowers businesses to efficiently analyze and report on their PostgreSQL data, ensuring timely and accurate insights.

The implementation leverages the "[Publish–subscribe pattern](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern)" (***PubSub***) to facilitate the smooth movement of data from **Postgres** to **ClickHouse**, enhancing data accessibility and analysis.

## Folder Structure

> The folder and file structure in the project is organized
> alphabetically rather than following a project hierarchy. This
> structure aligns with the functionality hierarchy and can be referred
> to in the "[How to Start](#how-to-start)" section for a clear
> understanding of the project's components and their order.

    .
    ├── cdc                                 # Change Data Capture (Debezium Configs)
        ├── connectors                      # Debezium Connectors
        ├── kafka                           # Scripts to monitor Kafka topics
        |── docker-compose.yml				# CDC images/containers

    ├── db                                  # About Databases (Postgres & Clickhouse)
        ├── olap							# Clickhouse section
            ├── ddl                       	# Tables and Views definition
            ├── ddm							# Ingest data from Postgres using scripts                                     
        ├── oltp							# Postgres section
            ├── ddl                       	# Tables definition
            ├── ddm							# Seed the db with random records
    ├──  img                                # BI Screenshots
    ├── .gitignore
    ├── README.md
    └── requirements.txt                    #  Python packages to be installed

*[Superset](https://superset.apache.org/) is not included in this repository. Instead, instructions for installing it as a separate service and integrating it with ClickHouse will be provided*


## How to Start

 ### Project Setup

Follow these steps to get started with  CDC:

1. Clone the Rembo CDC repository to your local machine using the following command:

```bash
git  clone  git@github.com:joelatiam/cdc.git
```

2. (Optional) Create/Start Python Virtual Environment:

- Create a virtual environment:
	```bash
	python  -m  venv  .venv
	```

- Activate the virtual environment:

	- On macOS and Linux:  

		```bash
		source  .venv/bin/activate
		```

3. Install dependencies:

	```bash
	pip  install  -r  requirements.txt
	```

### OLTP Database: Postgres

*Instructions on setting up and configuring the PostgreSQL database.* (Update your **oltp/.env**  following **oltp/.env.example**)

For the OLTP component, [database replication should be logical](https://hevodata.com/learn/postgresql-logical-replication/#:~:text=To%20perform%20logical%20replication%20in,conf%20file.), and the  **db user** needs to have replication privileges **(ALTER ROLE db_user WITH REPLICATION LOGIN;)**. Follow these steps to set up the OLTP component:
  
1.  Create tables:
	- To [create/recreate](db/oltp/ddl/create_tables.py) the necessary tables, run the following command:
		```bash
		python  db/oltp/ddl/tables_creations.py
		```
		
2.  Alter tables with [Replica Identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY):
	```bash
	python  db/oltp/ddl/alter_tables.py
	```
3. Populate (Fake) Records to the rembo db

	- We are generating random records *(**500k:** customer, sales | **11** sales_territory | **5** employee)* to be used in our project

	```bash
	python  db/oltp/dml/create_random_records.py
	```

These steps will ensure that the PostgreSQL database is ready for logical replication and data capture.

### OLAP Clickhouse Database: Tables Creations and Copy Postgres Records

*Instructions on setting up and configuring the Clickhouse database.* (Update your **olap/.env**  following **olap/.env.example**)

You can use this [approache](https://clickhouse.com/docs/en/install#quick-install) to install and run Clickhouse. Follow these steps to set up the OLAP component:
  
 -  Create tables:
	- To [create/recreate](db/olap/ddl/create_tables.py)  tables, run the following command:
	
		```bash
		python  db/olap/ddl/tables_creations.py
		```
		
	 - [db/olap/ddl/](db/olap/ddl/) directories contains:
		 - Tables definitaions
		 - [Kafka Table Engines](https://clickhouse.com/docs/en/integrations/kafka/kafka-table-engine) & Materielized Views
		 - Views helping to fetch most recent records versions *(This might not be the best approach since Clickhouse provides [rich functionalities](https://clickhouse.com/docs/en/sql-reference/statements/create/table) of organizing records in table in creation)*.
		
 -  Ingest records from OTLP (Postgres)

	- Since we are initializing our Postgres DB with many records *(**500k:** customer, sales | **11** sales_territory | **5** employee)*, getting all the records at once through kafka will be challenging in development environment, this script helps copying all the postgres records
	```bash
	python  db/olap/dml/import_from_oltp.py
	```

### CDC with Debezium and Kafka

We will use **Debezium** to capture changes from our **Postgres**  and publish to **Kafka** topics.

 - Start applications (**zookeeper**, **kafka**, **debezium**, [**kafdrop**](https://github.com/obsidiandynamics/kafdrop): *view kafka topics, groups on the browser: http://localhost:9090/*)
	```bash
	cd cdc
	```
	```bash
	docker-compose up -d
	```
 - Create Debezium connector
	[This json.example](cdc/connectors/pg-src.json.example) **(cdc/connectors/pg-src.json.example)** is an example of the connector we will create by posting the [request](https://docs.confluent.io/platform/current/connect/references/restapi.html) to [debezium](https://debezium.io/documentation/reference/stable/tutorial.html) *(you can use **postman**, **vscode thunder client**, ...)* . Remember to update your dabase.info if you use **cdc/connectors/pg-src.json.example**
	
	eg: **POST** http://localhost:8083/connectors
	```bash
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "inventory-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "debezium", "database.password": "dbz", "database.server.id": "184054", "topic.prefix": "dbserver1", "database.include.list": "inventory", "schema.history.internal.kafka.bootstrap.servers": "kafka:9092", "schema.history.internal.kafka.topic": "schemahistory.inventory" } }' 
	```
	Debezium provides other [endpoints](https://docs.confluent.io/platform/current/connect/references/restapi.html) to helping to manage the connector
 - Test Postgres to Kafka (Update records and watch kafka messages)

	 - Create/Update records from in our tables
	 - View/Read Kafka messages using **Kafkadrop** http://localhost:9090/
	 
		 **Message example**
		```bash
		{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"default":0,"field":"sales_id"},{"type":"int32","optional":true,"field":"customer_id"},{"type":"string","optional":true,"field":"discount_amount"}},"payload":{"sales_id":2, "customer_id":2496,"discount_amount":"85","duedate":"2023-10-19","extended_amount":843,"order_quantity":96,"product_standard_cost":22,"revision_number":62,"sales_amount":7,"sales_order_line_number":"2","sales_order_number":"2","sales_territory_id":3,"shipdate":19657,"tax_amt":55,"total_product_cost":77,"unit_price":21677,"employee_id":2,"__table":"sales","__op":"u","__lsn":12912566024,"__source_ts_ms":1698765730595,"__deleted":"false"}} 
		```
	

	 - (Optional) Listen to a kafka topic from the terminal.
		 We provided a python [script](cdc/kafka/watch-topics.py) (cdc/kafka/watch-topics.py) to watch a topic messages **(Remember to update the .env)**

		```bash
		python cdc/kafka/watch-topics.py 
		```

	

> Our Clickhouse DB will be connected to Kafka from the the folling section, so at this phase changes from OLTP DB to OLAP DB won't be reflected yet

### Databases Sync: Connect Clickhouse Tables to Kafka Topics

*Instructions on connecting clickhouse tables to kafka topics.* (Update your **olap/.env**  following **olap/.env.example**)

 -  Connect tables to kafka
	```bash
	python  db/olap/ddl/create_kafka_connect.py  
	```
From here, any Create, Update, Delete operation from postgres will be reflected to our clickhouse db.

### Data Visualization: Superset

You can install Superset by referring to the [vendor's official website](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/). We have adopted the "**git clone**" option and employed **Docker Compose** for running Superset. Depending on your specific requirements, you may need to make adjustments to the provided Docker Compose and **.env** files. For instance, you can disable their PostgreSQL database and configure Superset to use a different database, or modify the port settings.

To ensure stable operation, we have chosen to run Superset in production (**non-dev**) mode, avoiding a known [bug that affects the development version](https://github.com/apache/superset/issues/17076). 
If you decide to run Superset in production mode, the installation page we've shared contains comprehensive instructions. Additionally, you'll need to update the `SUPERSET_LOAD_EXAMPLES` variable in the `docker/.env-non-dev` file with a secret key, which you can generate using the command:

    openssl rand -base64 42

 - **Clickhouse Plugin**
	 Follow instruction founds [here](https://superset.apache.org/docs/databases/clickhouse/) to add clickhouse integration.
	 
 - **Datasets**
	 After establishing a connection to the ClickHouse database, you can  integrate the pre-defined **table views** as datasets, or even incorporate custom SQL queries as virtual tables. We've created four virtual tables, which serve as datasets *(refer to queries in the [OLAP Query Examples](#olap-query-examples) section)* to simplify the process of generating charts and visualizations.
	 
- **Dashboards** 
	We've used the queries from the [OLAP Query Examples](#olap-query-examples) section to generate the charts shown in the screenshots. You can adjust the dashboard's auto-refresh interval to instantly reflect any record changes.
	
<img  src="img/Screen Shot 2023-11-04 at 03.49.43-min.png"  alt="Screenshot"  width="800"  height="400">

<img  src="img/Screen Shot 2023-11-04 at 03.50.58-min.png"  alt="Screenshot"  width="800"  height="400">



## Scripts List

| Script Name                                          | Description                                                       |
|-----------------------------------------------------|-------------------------------------------------------------------|
| `db/oltp/ddl/create_tables.py` (OLTP Postgres)   | Python script to create or recreate the necessary tables in the OLTP Postgres database. |
| `db/oltp/ddl/alter_tables.py` (OLTP Postgres)      | Python script to alter tables with Replica Identity in the OLTP Postgres database. |
| `db/oltp/dml/create_random_records.py` (OLTP Postgres) | Python script to generate random records for customer, sales, sales territory, and employee tables in the OLTP Postgres database. |
| `db/oltp/dml/create_sales_record.py` (OLTP Postgres) | Python script to generate a random sales records *(This can be used to observe how the record is created and reflected to the dashboard in near real time)*. |
| `db/olap/ddl/create_tables.py` (OLAP Clickhouse) | Python script to create or recreate tables and views in the OLAP Clickhouse database. |
| `db/olap/dml/import_from_oltp.py` (OLAP Clickhouse) | Python script to copy records from the OLTP Postgres database to the OLAP Clickhouse database. |
| `db/olap/ddl/create_kafka_connect.py` (OLAP Clickhouse) | Python script to connect Clickhouse tables to Kafka topics for change data capture. |
| `cdc/kafka/watch-topics.py` (CDC with Debezium and Kafka) | Python script to watch Kafka topics for messages related to changes in the Postgres database. |


### Notes

- These scripts are used in different phases of the data integration process, from setting up the OLTP and OLAP databases to managing change data capture (CDC) with Debezium and Kafka.
- Ensure that you run these scripts from the project's root directory and have the required environment variables and configurations set up.
- Be cautious when running these scripts, especially in a production environment, as they may modify database records or structures.

Please refer to the script descriptions for more information on their usage and purpose.




## Data Dictionary: Clickhouse OLAP

### Notes

> The combination of  `(table_name)_id`  and `record_version` column serves as the primary key for each table, ensuring the uniqueness of each record per create/update/delete, so fetching a record id + the highest `record_version`, returns the most recent version/state of the record. Some columns are marked as "Nullable," indicating they can contain null values, representing missing or unknown information. 
> The `arrival_date_time_tz` column is set to the default value of the current timestamp when a new record is inserted into the warehouse. 
> The `source_updated_at_ms` represents the `ts_ms` (timestamp in millisecond) from the Postgres WAL or [from  Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) capture if the source DB didn't provide
> The `db_action` column can be used to track the type of operation that was performed
> on the record (e.g., insertion, update, deletion).


 -  ### sales_territory

| Column Name             | Data Type           | Description                                                      |
|-------------------------|---------------------|------------------------------------------------------------------|
| sales_territory_id      | Int32               | Unique identifier for the sales territory.                        |
| sales_territory_country | Nullable String     | Country associated with the sales territory.                     |
| sales_territory_region  | Nullable String     | Region or area within the country.                                |
| sales_territory_city    | Nullable String     | City within the region.                                           |
| created_at              | Nullable DateTime   | Timestamp of when the sales territory was created.               |
| record_version          | Int64               | Integer representing the record version.                         |
| arrival_date_time_tz    | DateTime            | Timestamp of insertion or update in the server's time zone.      |
| source_updated_at_ms    | Nullable Int64      | Timestamp in milliseconds of the source data's last update.      |
| db_action               | Nullable String     | Description of the database operation (e.g., 'insert', 'update'). |
| is_deleted              | Nullable UInt8      | Flag indicating record deletion (1 for deleted, 0 for not deleted).|


-  ### employee

| Column Name             | Data Type           | Description                                                      |
|-------------------------|---------------------|------------------------------------------------------------------|
| employee_id             | Int32               | Unique identifier for the employee.                               |
| employee_name           | Nullable String     | Name of the employee.                                             |
| sales_territory_id      | Nullable Int32      | Sales territory associated with the employee.                    |
| created_at              | Nullable DateTime   | Timestamp of when the employee was created.                      |
| record_version          | Nullable Int64      | Integer representing the record version.                         |
| arrival_date_time_tz    | DateTime            | Timestamp of insertion or update in the server's time zone.      |
| source_updated_at_ms    | Nullable Int64      | Timestamp in milliseconds of the source data's last update.      |
| db_action               | Nullable String     | Description of the database operation (e.g., 'insert', 'update'). |
| is_deleted              | Nullable UInt8      | Flag indicating record deletion (1 for deleted, 0 for not deleted).|


-  ### customer

| Column Name                | Data Type           | Description                                                      |
|----------------------------|---------------------|------------------------------------------------------------------|
| customer_id                | Int32               | Unique identifier for the customer.                               |
| last_name                  | Nullable String     | Last name of the customer.                                        |
| address_line1              | Nullable String     | First line of the customer's address.                             |
| address_line2              | Nullable String     | Second line of the customer's address.                            |
| birth_date                 | Nullable Date       | Date of birth of the customer.                                    |
| commute_distance           | Nullable Int32      | Distance of the customer's commute.                               |
| customer_alternate_key     | Nullable String     | Alternate key for the customer.                                   |
| date_first_purchase        | Nullable String     | Date of the customer's first purchase.                            |
| email_address              | Nullable String     | Email address of the customer.                                   |
| education                  | Nullable String     | Educational background of the customer.                          |
| occupation                 | Nullable String     | Occupation of the customer.                                       |
| first_name                 | Nullable String     | First name of the customer.                                       |
| gender                     | Nullable String     | Gender of the customer.                                           |
| house_owner_flag           | Nullable UInt8      | Flag indicating if the customer is a house owner (1 or 0).        |
| marital_status             | Nullable String     | Marital status of the customer.                                  |
| middle_name                | Nullable String     | Middle name of the customer.                                     |
| name_style                 | Nullable String     | Style of the customer's name.                                    |
| number_cars_owned          | Nullable Int32      | Number of cars owned by the customer.                            |
| number_children_at_home    | Nullable Int32      | Number of children at home for the customer.                      |
| phone                      | Nullable String     | Phone number of the customer.                                    |
| suffix                     | Nullable String     | Suffix for the customer's name.                                  |
| title                      | Nullable String     | Title for the customer (e.g., Mr., Mrs.).                          |
| total_children             | Nullable String     | Total number of children for the customer.                        |
| yearly_income              | Nullable Int32      | Yearly income of the customer.                                   |
| created_at                 | Nullable DateTime   | Timestamp of when the customer was created.                      |
| record_version             | Nullable Int64      | Integer representing the record version.                         |
| arrival_date_time_tz       | DateTime            | Timestamp of insertion or update in the server's time zone.      |
| source_updated_at_ms       | Nullable Int64      | Timestamp in milliseconds of the source data's last update.      |
| db_action                  | Nullable String     | Description of the database operation (e.g., 'insert', 'update'). |
| is_deleted                 | Nullable UInt8      | Flag indicating record deletion (1 for deleted, 0 for not deleted).|


-  ### sales

| Column Name                | Data Type           | Description                                                      |
|----------------------------|---------------------|------------------------------------------------------------------|
| sales_id                   | UInt32              | Unique identifier for the sales record.                            |
| currencykey                | Nullable String     | Currency key associated with the sale.                             |
| customer_id                | Nullable Int32      | Customer ID associated with the sale.                              |
| discount_amount            | Nullable String     | Amount of discount applied to the sale.                            |
| duedate                    | Nullable Date       | Due date for the sale.                                             |
| duedatekey                 | Nullable Int32      | Key representing the due date.                                     |
| extended_amount            | Nullable Int32      | Extended amount of the sale.                                      |
| freight                   | Nullable Int32       | Freight cost for the sale.                                        |
| order_date                 | Nullable DateTime   | Date of the sale order.                                           |
| order_quantity             | Nullable Int32      | Quantity of items in the sale order.                               |
| product_standard_cost      | Nullable Int32      | Standard cost of the product in the sale.                          |
| revision_number            | Nullable Int32      | Revision number of the sale.                                      |
| sales_amount               | Nullable Int32      | Total sales amount.                                               |
| sales_order_line_number    | Nullable String     | Line number of the sales order.                                   |
| sales_order_number         | Nullable String     | Sales order number.                                               |
| sales_territory_id         | Nullable Int32      | Sales territory associated with the sale.                         |
| shipdate                   | Nullable DateTime   | Ship date for the sale.                                           |
| tax_amt                   | Nullable Int32       | Tax amount for the sale.                                          |
| total_product_cost         | Nullable Int32      | Total product cost for the sale.                                  |
| unit_price                | Nullable Int32       | Unit price of the product in the sale.                            |
| unit_price_discount_pct    | Nullable Int32      | Discount percentage applied to the unit price.                    |
| employee_id                | Nullable Int32      | Employee ID associated with the sale.                             |
| created_at                 | Nullable DateTime   | Timestamp of when the sale was created.                            |
| record_version             | Int64               | Integer representing the record version.                         |
| arrival_date_time_tz       | DateTime            | Timestamp of insertion or update in the server's time zone.      |
| source_updated_at_ms       | Nullable Int64      | Timestamp in milliseconds of the source data's last update.      |
| db_action                  | Nullable String     | Description of the database operation (e.g., 'insert', 'update'). |
| is_deleted                 | Nullable UInt8      | Flag indicating record deletion (1 for deleted, 0 for not deleted).|


## OLAP Query Examples

 - **Company Sales**
 
	 This SQL query retrieves and consolidates sales data, connecting it with **customer** and **employee** details to provide comprehensive insights into the **sales** transactions. It utilizes **views** to access the latest records and join relevant information, making it suitable for generating detailed sales reports.
```
SELECT
s.sales_id sales_id, s.sales_amount sales_amount, s.order_date order_date,
s.customer_id customer_id,
c.first_name AS customer_first_name,
c.last_name AS customer_last_name,
c.email_address AS customer_email_address,
c.education AS customer_education,
c.gender AS customer_gender,
c.phone AS customer_phone,
st.sales_territory_country sales_territory_country, st.sales_territory_city sales_territory_city,
emp.employee_id employee_id, emp.employee_name employee_name
FROM view_sales_last_rec s
LEFT JOIN view_customer_last_rec c ON s.customer_id = c.customer_id
LEFT JOIN view_employees_last_rec emp ON s.employee_id = emp.employee_id
LEFT JOIN view_sales_territory_last_rec st ON s.sales_territory_id = st.sales_territory_id;
```

- **Sales Per Customer Per Location**
	This SQL query generates a summarized report of **sales** data, including **customer** details like their first name, last name, gender, email address, and **sales territory** information. It aggregates **sales average time(hours) between sales** **sales amounts**, counts **sales records**, and calculates the **average sales amount per customer**. The results are grouped by customer and sales territory, providing insights into sales performance.
```
SELECT `customer_id` AS `customer_id`,
`customer_first_name` AS `customer_first_name`,
`customer_last_name` AS `customer_last_name`,
sum(`sales_amount`) AS `SUM(sales_amount)`,
count() AS `COUNT(sales_id)`,
ROUND(AVG(`purchase_time_diff_in_hours`), 2) AS `AVG(purchase_time_diff_in_hours)`,
AVG(`sales_amount`) AS `AVG(sales_amount)`,
`sales_territory_city` AS `sales_territory_city`,
`sales_territory_country` AS `sales_territory_country`,
`customer_gender` AS `customer_gender`,
`customer_email_address` AS `customer_email_address`
FROM
(SELECT s.sales_id sales_id,
s.sales_amount sales_amount,
s.order_date order_date,
s.customer_id customer_id,
(sales_with_purchase_time.purchase_time_diff_ms/ 3600000) AS purchase_time_diff_in_hours,
c.first_name AS customer_first_name,
c.last_name AS customer_last_name,
c.email_address AS customer_email_address,
c.education AS customer_education,
c.gender AS customer_gender,
c.phone AS customer_phone,
st.sales_territory_country sales_territory_country,
st.sales_territory_city sales_territory_city,
emp.employee_id employee_id,
emp.employee_name employee_name
FROM view_sales_last_rec s
LEFT JOIN view_sales_last_rec_with_time_between_purchase_in_region sales_with_purchase_time ON s.sales_id = sales_with_purchase_time.sales_id
LEFT JOIN view_customer_last_rec c ON s.customer_id = c.customer_id
LEFT JOIN view_employees_last_rec emp ON s.employee_id = emp.employee_id
LEFT JOIN view_sales_territory_last_rec st ON s.sales_territory_id = st.sales_territory_id) AS `virtual_table`
GROUP BY `customer_id`,
`customer_first_name`,
`customer_last_name`,
`customer_gender`,
`customer_email_address`,
`sales_territory_country`,
`sales_territory_city`
ORDER BY sales_territory_country,
sales_territory_city,
AVG(`purchase_time_diff_in_hours`) ASC,
SUM(sales_amount) DESC,
COUNT() DESC;
```

- **Employees Sales Per City**
	This query combines **sales statistics** for **employees** in different sales territories. It calculates the sum of **sales amounts** and **sales count** for each employee in their respective territory, displaying sales territory details and employee information. If there are no sales for a specific territory or employee, it includes zero values for sales amount and count.
```
WITH employee_sales AS
(
SELECT
emp.employee_id AS employee_id,SUM(s.sales_amount) sales_amount,
COALESCE(COUNT(s.sales_id), 0) sales_count,s.sales_territory_id AS sales_territory_id
FROM view_employees_last_rec emp
INNER JOIN view_sales_last_rec s ON emp.employee_id = s.employee_id
GROUP BY (emp.employee_id, s.sales_territory_id)
),
st_with_emp AS (
SELECT st.sales_territory_id,st.sales_territory_country, st.sales_territory_city,
emp.employee_id, emp.employee_name
FROM view_sales_territory_last_rec st
FULL OUTER JOIN view_employees_last_rec emp ON true ORDER BY st.sales_territory_id
)
SELECT
st.sales_territory_id, st.sales_territory_country, st.sales_territory_city,
st.employee_id, st.employee_name,COALESCE(emp_s.sales_amount, 0)sales_amount,
COALESCE(emp_s.sales_count, 0) sales_count
FROM st_with_emp st
LEFT JOIN employee_sales emp_s ON st.sales_territory_id = emp_s.sales_territory_id
AND  st.employee_id = emp_s.employee_id;
```

- **Current Employees**
	This SQL query retrieves the most recent employee and sales territory details, filtering out deleted records to ensure data accuracy.

```	
WITH emp AS (
SELECT 
DISTINCT ON (e.employee_id) e.employee_id, e.employee_name, e.sales_territory_id, e.created_at, 
e.arrival_date_time_tz, e.source_updated_at_ms, e.is_deleted, e.record_version
FROM employee e
ORDER BY e.employee_id, e.record_version DESC
),
st AS (
SELECT
DISTINCT ON (st.sales_territory_id) st.sales_territory_id, st.sales_territory_country,
st.sales_territory_region, st.sales_territory_city, st.record_version, st.is_deleted 
FROM sales_territory st 
ORDER BY st.sales_territory_id, st.record_version DESC
)
SELECT emp.*, st.sales_territory_country, st.sales_territory_region, st.sales_territory_city
FROM emp
INNER JOIN st ON emp.sales_territory_id = st.sales_territory_id
WHERE emp.is_deleted = 0 AND st.is_deleted = 0
```
